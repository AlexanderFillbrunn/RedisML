from redis import Redis
import numpy
import json
import redisml.shared.matrix_serialization as ser
import redisml.shared.redis_constants as const

class RedisWrapper:

    def __init__(self, red):
        self.redis = red
        self.slaves = {}
    
    def get_slave_redis(self, slave_name):
        slave_redis = None
        if self.slaves.has_key(slave_name):
            slave_redis = self.slaves[slave_name]
        else:
            slave_info = json.loads(self.redis.get('slave:' + slave_name))
            slave_redis = Redis(slave_info['host'], port=slave_info['port'], db=slave_info['db'])
            self.slaves[slave_name] = slave_redis
        return slave_redis
    
    def create_block(self, block_name, data):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        slave_redis.set(block_name, data.dumps())
        
    def get_block(self, block_name):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        return numpy.loads(slave_redis.get(block_name))
        
    def delete_block(self, block_name):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        slave_redis.delete(block_name)