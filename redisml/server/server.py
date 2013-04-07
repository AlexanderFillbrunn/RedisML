import matrix
import json
from redis import Redis
import redisml.shared.redis_constants as const
from redisml.shared.keymanager import KeyManager

class Server:
    def __init__(self, config):
        self.name = config['server_name']
        self.redis = Redis(config['redis_master']['host'], port=config['redis_master']['port'], db=config['redis_master']['db'])
        self.__init_redis(config)
        self.matrix_factory = matrix.MatrixFactory(self.redis, self.key_mngr, config['matrix']['block_size'])
        
    def matrix_from_numpy(self, mat, name=''):
        return self.matrix_factory.matrix_from_numpy(mat, name)
        
    def matrix_from_name(self, name):
        return self.matrix_factory.matrix_from_name(name)
        
    def __init_redis(self, config):
        # Reset client ids
        self.redis.set('client_id', 0)
        # Clear the list of free jobs
        self.redis.delete(const.FREE_JOBS_KEY.format(self.name))
        self.redis.delete(const.SLAVE_LIST_KEY.format(self.name))
        slaves = []
        for k,v in config['redis_slaves'].items():
            self.redis.set(const.SLAVE_KEY.format(self.name, k), json.dumps(v))
            self.redis.lpush(const.SLAVE_LIST_KEY.format(self.name), k)
            slaves.append(k)
        self.key_mngr = KeyManager(slaves, self.name)