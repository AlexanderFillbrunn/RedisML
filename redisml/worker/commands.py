import numpy
import time
import math
import logging
import json
import pickle
import scipy.stats as stats
from redis import Redis
from redisml.shared import redis_constants as const
from redisml.shared.redis_wrapper import RedisWrapper

def _get_redis_slave(cmd_ctx, name):
    key = const.SLAVE_KEY.format(cmd_ctx.key_manager.server_name, name)
    slave_info = json.loads(cmd_ctx.redis_master.get(key))
    slave = Redis(slave_info['host'], slave_info['port'], slave_info['db'])
    return slave

def _get_matrix_block(cmd_ctx, block_name):
    slave_name = block_name.split(":")[1]
    return numpy.loads(_get_redis_slave(cmd_ctx, slave_name).get(block_name))
    
def _save_matrix_block(cmd_ctx, block_name, data):
    slave = _get_redis_slave(cmd_ctx, const.get_slave_name(block_name))
    slave.set(block_name, data.dumps())
    
def fail(cmd_ctx):
    raise Exception('Fail test')
    
def timeout(cmd_ctx):
    time.sleep(int(cmd_ctx.cmdArgs[0]))

def mtrans(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    res = m.transpose()
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1], res)

def mmult(cmd_ctx):
    if len(cmd_ctx.cmdArgs) < 4:
        raise Exception('Too few arguments')
    
    modifiers = cmd_ctx.cmdArgs[0].split(";")
    
    m1 = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1])
    if cmd_ctx.cmdArgs[1] == cmd_ctx.cmdArgs[2]:
        m2 = m1
    else:
        m2 = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2])
    
    if modifiers[0][0] != "n":
        for m in modifiers[0]:
            if m == "-":
                m1 = -m1
            if m == "t":
                m1 = m1.transpose()
    if modifiers[1][0] != "n":
        for m in modifiers[1]:
            if m == "-":
                m2 = -m2
            if m == "t":
                m2 = m2.transpose()
    
    m3 = m1.dot(m2)
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[3], m3)

def madd(cmd_ctx):
    if len(cmd_ctx.cmdArgs) < 1:
        raise Exception('Too few arguments')
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    
    if len(cmd_ctx.cmdArgs) > 2:
        for i in cmd_ctx.cmdArgs[1:len(cmd_ctx.cmdArgs)-1]:
            m += _get_matrix_block(cmd_ctx, i)

    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[len(cmd_ctx.cmdArgs)-1], m)
    
def ms(cmd_ctx):
    scalar = float(cmd_ctx.cmdArgs[0])
    op = cmd_ctx.cmdArgs[1]
    matrix = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2])
    
    if op == "*":
        result = matrix * scalar
    elif op == "/":
        result = matrix / scalar
    elif op == "+":
        result = matrix + scalar
    elif op == "-":
        result = matrix - scalar
        
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[3], result)

def colsum(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    #if cmd_ctx.cmdArgs[1] == 'x':
    #    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2], m.sum(axis=0))
    #else:
    code = compile(cmd_ctx.cmdArgs[1], '', 'eval')
    res = numpy.empty([2, m.shape[1]])
    for i in range(0, m.shape[1]):
        s = 0
        for x in m[:,i]:
            s += eval(code)
            res[0,i] = s
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2], res)
    
def rowsum(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    #if cmd_ctx.cmdArgs[1] == 'x':
    #    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2], m.sum(axis=1))
    #else:
    code = compile(cmd_ctx.cmdArgs[1], '', 'eval')
    res = numpy.empty([m.shape[0], 2])
    for i in range(0, m.shape[0]):
        s = 0
        for x in m[i,:]:
            s += eval(code)
        res[i,0] = s
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2], res)

def mtrace(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    cmd_ctx.redis_master.rpush(cmd_ctx.cmdArgs[1], numpy.trace(m))

def mrand(cmd_ctx):
    pass

def delete(cmd_ctx):
    redwrap = RedisWrapper(cmd_ctx.redis_master, cmd_ctx.key_manager)
    for m in cmd_ctx.cmdArgs:
        redwrap.delete_block(m)

def equal(cmd_ctx):
    key = cmd_ctx.cmdArgs[2]
    # Same block in redis means we do not have to compare
    if cmd_ctx.cmdArgs[0] == cmd_ctx.cmdArgs[1]:
        cmd_ctx.redis_master.rpush(key, 1)
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    n = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1])
    if len(cmd_ctx.cmdArgs) > 3:
        eps = float(cmd_ctx.cmdArgs[2])
    else:
        eps = 1e-6
    t = m - n
    max = numpy.max(numpy.abs(t))
    if max > eps:
        cmd_ctx.redis_master.rpush(key, 0)
    else:
        cmd_ctx.redis_master.rpush(key, 1)
        
def k_means_distance(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    v = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1])
    key = cmd_ctx.cmdArgs[len(cmd_ctx.cmdArgs)-1]
    num_v = len(v)
    num_r = len(m)
    result = numpy.empty((num_r,num_v))
    for v_row in range(0,num_v):
        for m_row in range(0,num_r):
            sum = 0
            for j in range(0, m.shape[1]):
                sum += (m[m_row,j]-v[v_row,j])**2
            result[m_row,v_row] = sum
    tmp = cmd_ctx.redis_master.lpop(key)
    if tmp == None:
        cmd_ctx.redis_master.rpush(key, result.dumps())
    else:
        cmd_ctx.redis_master.rpush(key, (result + numpy.loads(tmp)).dumps())

def cw(cmd_ctx):
    op = cmd_ctx.cmdArgs[0]
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1])
    if cmd_ctx.cmdArgs[1] == cmd_ctx.cmdArgs[2]:
        n = m
    else:
        n = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[2])
    if op == "+":
        res = m+n
    elif op == "-":
        res = m-n
    elif op == "/":
        res = m/n
    elif op == "*":
        res = m*n
    else:
        raise Exception('Unknown operator ' + op)
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[3], res)

def count(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    key = cmd_ctx.cmdArgs[1]
    pipe = cmd_ctx.redis_master.pipeline()
    for i in range(0, m.shape[1]):
        freq = stats.itemfreq(m[:,i])
        pref = key + ':' + str(i)
        for row in freq:
            val = row[0]
            num = int(row[1])
            pipe.sadd(pref, val) # Add value to set of found values in this column
            pipe.incr(pref + ':' + str(val), num) # Increase counter for the value in this column
    pipe.execute()
    
def mcreate(cmd_ctx):
    fill = cmd_ctx.cmdArgs[0]
    rows = int(cmd_ctx.cmdArgs[1])
    cols = int(cmd_ctx.cmdArgs[2])
    m = None
    if fill.lower() == 'rand':
        m = numpy.random.rand(rows, cols)
    if fill == '0':
        m = numpy.zeros((rows, cols))
    else:
        m = numpy.ones((rows, cols)) * double(fill)
    _save_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[3], m)
        
def k_means_recalc(cmd_ctx):
    m = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[0])
    d = _get_matrix_block(cmd_ctx, cmd_ctx.cmdArgs[1])
    result_prefix = cmd_ctx.cmdArgs[2]
    #Only count if prefix is given
    counter_prefix = None
    if len(cmd_ctx.cmdArgs) > 3:
        counter_prefix = cmd_ctx.cmdArgs[3]
    result = {}
    for row in range(0,len(d)):
        min = None
        min_col = -1
        for col in range(0,len(d[row])):
            if min == None or d[row,col] < min:
                min = d[row,col]
                min_col = col
        if counter_prefix != None:
            cmd_ctx.redis_master.incr(counter_prefix + str(min_col))
        if result.has_key(min_col):
            result[min_col] += m[row]
        else:
            result[min_col] = m[row]
            
    for key in result.keys():
        k = result_prefix + str(key)
        tmp = cmd_ctx.redis_master.lpop(k)
        if tmp == None:
            cmd_ctx.redis_master.rpush(k, result[key].dumps())
        else:
            cmd_ctx.redis_master.rpush(k, (result[key] + numpy.loads(tmp)).dumps())