from redis import Redis
import numpy
import json
import redisml.shared.matrix_serialization as ser
import redisml.shared.redis_constants as const

class RedisWrapper:

    def __init__(self, red, key_mngr):
        self.redis = red
        self.slaves = {}
        self.key_mngr = key_mngr
    
    def get_slave_redis(self, slave_name):
        slave_redis = None
        if self.slaves.has_key(slave_name):
            slave_redis = self.slaves[slave_name]
        else:
            slave_info = json.loads(self.redis.get(const.SLAVE_KEY.format(self.key_mngr.server_name, slave_name)))
            slave_redis = Redis(slave_info['host'], port=slave_info['port'], db=slave_info['db'])
            self.slaves[slave_name] = slave_redis
        return slave_redis
    
    def create_block(self, block_name, data):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        slave_redis.set(block_name, data.dumps())
        
    def get_block(self, block_name):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        return numpy.loads(slave_redis.get(block_name))
    
    def get_value(self, key):
        slave_redis = self.get_slave_redis(const.get_slave_name(key))
        return slave_redis.get(key)
        
    def set_value(self, key, value):
        slave_redis = self.get_slave_redis(const.get_slave_name(key))
        return slave_redis.set(key, value)
     
    def delete_block(self, block_name):
        slave_redis = self.get_slave_redis(const.get_slave_name(block_name))
        slave_redis.delete(block_name)