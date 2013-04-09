import numpy
import json
import redisml.shared.redis_constants as const
from redisml.shared.redis_wrapper import RedisWrapper

def file2redis(name, rows, cols, redis, key_manager, file_format, block_size=None):
    # Handle 0,0 block first to guess the block size
    rw = RedisWrapper(redis, key_manager)
    
    file = file_format.format(0, 0)
    matrix = numpy.genfromtxt(file ,delimiter=';',dtype=None)
    rw.create_block(key_manager.get_block_name(name, 0, 0), matrix)
    if block_size == None:
        block_size = matrix.shape[0]

    redis.hmset(const.INFO_FORMAT.format(name), { 'block_size': block_size, 'rows' : rows, 'cols' : cols })
    
    for row in range(0, rows / block_size):
        for col in range(0, cols / block_size):
            if not (row == col and row == 0):
                file = file_format.format(row, col)
                matrix = numpy.genfromtxt(file ,delimiter=';',dtype=None)
                rw.create_block(key_manager.get_block_name(name, row, col), matrix)
            
def redis2file(name, redis, key_manager, file_format):
    rw = RedisWrapper(redis, key_manager)
    info = redis.hgetall(const.INFO_FORMAT.format(name))
    rows = int(info['rows'])
    cols = int(info['cols'])
    block_size = int(info['block_size'])
    for row in range(0, rows / block_size):
        for col in range(0, cols / block_size):
            file = file_format.format(row, col)
            matrix = rw.get_block(key_manager.get_block_name(name, row, col))
            numpy.savetxt(file, matrix, delimiter=';', fmt='%f')
