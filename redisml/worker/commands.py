import numpy
import time
import math
import logging
import json
import pickle
from redis import Redis
from redisml.shared import redis_constants as const
from redisml.shared.redis_wrapper import RedisWrapper

def _get_redis_slave(redis_master, name):
    slave_info = json.loads(redis_master.get('slave:' + name))
    slave = Redis(slave_info['host'], slave_info['port'], slave_info['db'])
    return slave

def _get_matrix_block(redis_master, block_name):
    slave_name = block_name.split(":")[1]
    return numpy.loads(_get_redis_slave(redis_master, slave_name).get(block_name))
    
def _save_matrix_block(redis_master, block_name, data):
    slave = _get_redis_slave(redis_master, const.get_slave_name(block_name))
    slave.set(block_name, data.dumps())

def fail(r, cmdArgs):
    raise Exception('Fail test')
    
def timeout(r, cmdArgs):
    time.sleep(int(cmdArgs[0]))

def mtrans(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    res = m.transpose()
    _save_matrix_block(r, cmdArgs[1], res)

def mmult(r, cmdArgs):
    if len(cmdArgs) < 4:
        raise Exception('Too few arguments')
    
    modifiers = cmdArgs[0].split(";")
    
    m1 = _get_matrix_block(r, cmdArgs[1])
    if cmdArgs[1] == cmdArgs[2]:
        m2 = m1
    else:
        m2 = _get_matrix_block(r, cmdArgs[2])
    
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
    _save_matrix_block(r, cmdArgs[3], m3)

def madd(r, cmdArgs):
    if len(cmdArgs) < 1:
        raise Exception('Too few arguments')
    m = _get_matrix_block(r, cmdArgs[0])
    
    if len(cmdArgs) > 2:
        for i in cmdArgs[1:len(cmdArgs)-1]:
            m += _get_matrix_block(r, i)

    _save_matrix_block(r, cmdArgs[len(cmdArgs)-1], m)
    
def ms(r, cmdArgs):
    scalar = float(cmdArgs[0])
    op = cmdArgs[1]
    matrix = _get_matrix_block(r, cmdArgs[2])
    
    if op == "*":
        result = matrix * scalar
    elif op == "/":
        result = matrix / scalar
    elif op == "+":
        result = matrix + scalar
    elif op == "-":
        result = matrix - scalar
        
    _save_matrix_block(r, cmdArgs[3], result)

def colsum(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    #if cmdArgs[1] == 'x':
    #    _save_matrix_block(r, cmdArgs[2], m.sum(axis=0))
    #else:
    code = compile(cmdArgs[1], '', 'eval')
    res = numpy.empty([2, m.shape[1]])
    for i in range(0, m.shape[1]):
        s = 0
        for x in m[:,i]:
            s += eval(code)
            res[0,i] = s
    _save_matrix_block(r, cmdArgs[2], res)
    
def rowsum(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    #if cmdArgs[1] == 'x':
    #    _save_matrix_block(r, cmdArgs[2], m.sum(axis=1))
    #else:
    code = compile(cmdArgs[1], '', 'eval')
    res = numpy.empty([m.shape[0], 2])
    for i in range(0, m.shape[0]):
        s = 0
        for x in m[i,:]:
            s += eval(code)
        res[i,0] = s
    _save_matrix_block(r, cmdArgs[2], res)

def mtrace(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    r.rpush(cmdArgs[1], numpy.trace(m))

def mrand(r, cmdArgs):
    pass

def delete(r, cmdArgs):
    redwrap = RedisWrapper(r)
    for m in cmdArgs:
        redwrap.delete_block(m)

def equal(r, cmdArgs):
    key = cmdArgs[2]
    # Same block in redis means we do not have to compare
    if cmdArgs[0] == cmdArgs[1]:
        r.rpush(key, 1)
    m = _get_matrix_block(r, cmdArgs[0])
    n = _get_matrix_block(r, cmdArgs[1])
    if len(cmdArgs) > 3:
        eps = float(cmdArgs[2])
    else:
        eps = 1e-6
    t = m - n
    max = numpy.max(numpy.abs(t))
    if max > eps:
        r.rpush(key, 0)
    else:
        r.rpush(key, 1)
        
def k_means_distance(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    v = _get_matrix_block(r, cmdArgs[1])
    key = cmdArgs[len(cmdArgs)-1]
    num_v = len(v)
    num_r = len(m)
    result = numpy.empty((num_r,num_v))
    for v_row in range(0,num_v):
        for m_row in range(0,num_r):
            sum = 0
            for j in range(0, m.shape[1]):
                sum += (m[m_row,j]-v[v_row,j])**2
            result[m_row,v_row] = sum
    tmp = r.lpop(key)
    if tmp == None:
        r.rpush(key, result.dumps())
    else:
        r.rpush(key, (result + numpy.loads(tmp)).dumps())

def cw(r, cmdArgs):
    op = cmdArgs[0]
    m = _get_matrix_block(r, cmdArgs[1])
    if cmdArgs[1] == cmdArgs[2]:
        n = m
    else:
        n = _get_matrix_block(r, cmdArgs[2])
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
    _save_matrix_block(r, cmdArgs[3], res)

def count(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    key = cmdArgs[1]
    pipe = r.pipeline()
    for i in range(0, m.shape[1]):
        for j in range(0, m.shape[0]):
            val = m[j,i]
            pref = key + ':' + str(i)
            pipe.sadd(pref, val) # Add value to set of found values in this column
            pipe.incr(pref + ':' + str(val)) # Increase counter for the value in this column
    pipe.execute()
    
def k_means_recalc(r, cmdArgs):
    m = _get_matrix_block(r, cmdArgs[0])
    d = _get_matrix_block(r, cmdArgs[1])
    result_prefix = cmdArgs[2]
    #Only count if prefix is given
    counter_prefix = None
    if len(cmdArgs) > 3:
        counter_prefix = cmdArgs[3]
    result = {}
    for row in range(0,len(d)):
        min = None
        min_col = -1
        for col in range(0,len(d[row])):
            if min == None or d[row,col] < min:
                min = d[row,col]
                min_col = col
        if counter_prefix != None:
            r.incr(counter_prefix + str(min_col))
        if result.has_key(min_col):
            result[min_col] += m[row]
        else:
            result[min_col] = m[row]
            
    for key in result.keys():
        k = result_prefix + str(key)
        tmp = r.lpop(k)
        if tmp == None:
            r.rpush(k, result[key].dumps())
        else:
            r.rpush(k, (result[key] + numpy.loads(tmp)).dumps())