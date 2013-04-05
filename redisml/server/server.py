import matrix
import json
from redis import Redis
from keymanager import KeyManager

class Server:
    def __init__(self, config):
        self.name = config['server_name']
        self.redis = Redis(config['redis_master']['host'], port=config['redis_master']['port'], db=config['redis_master']['db'])
        self.matrix_factory = matrix.MatrixFactory(self.redis, config['matrix']['block_size'])
        self.__init_redis(config)
        
    def matrix_from_numpy(self, mat, name=''):
        return self.matrix_factory.matrix_from_numpy(mat, self.key_mngr, name)
        
    def matrix_from_name(self, name):
        return self.matrix_factory.matrix_from_name(name, self.key_mngr)
        
    def __init_redis(self, config):
        # Reset client ids
        self.redis.set('client_id', 0)
        # Clear the list of free jobs
        self.redis.delete('free_jobs')
        self.redis.delete('slaves')
        slaves = []
        for k,v in config['redis_slaves'].items():
            self.redis.set('slave:' + k, json.dumps(v))
            self.redis.lpush('slaves', k)
            slaves.append(k)
        self.key_mngr = KeyManager(slaves)