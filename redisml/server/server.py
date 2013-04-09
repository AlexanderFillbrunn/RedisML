import matrix
import json
from redis import Redis
import redisml.shared.redis_constants as const
from redisml.shared.keymanager import KeyManager
from redisml.shared.redis_wrapper import RedisWrapper

class Context:
    def __init__(self, redis_master, key_manager, block_size, job_timeout, max_job_execs):
        self.redis_master = redis_master
        self.key_manager = key_manager
        self.block_size = block_size
        self.job_timeout = job_timeout
        self.max_job_execs = max_job_execs

class Server:
    def __init__(self, config):
        self.name = config['server_name']
        self.redis_master = Redis(config['redis_master']['host'], port=config['redis_master']['port'], db=config['redis_master']['db'])
        self.__init_redis(config)
        self.key_manager = self.__create_key_manager(config)
        self.context = Context(self.redis_master, self.key_manager,
                                config['matrix']['block_size'],
                                config['jobs']['timeout'],
                                config['jobs']['max_execs'])
        self.matrix_factory = matrix.MatrixFactory(self.context)
        
    def matrix_from_numpy(self, mat, name=''):
        return self.matrix_factory.matrix_from_numpy(mat, name)
        
    def matrix_from_name(self, name):
        return self.matrix_factory.matrix_from_name(name)
    
    def delete_matrix(self, name):
        info_key = const.INFO_FORMAT.format(name)
        info = self.redis_master.hgetall(info_key)
        rows = int(info['rows'])
        cols = int(info['cols'])
        block_size = int(info['block_size'])
        redwrap = RedisWrapper(self.redis_master, self.key_manager)
        for row in range(0, rows / block_size):
            for col in range(0, cols / block_size):
                block_name = self.key_manager.get_block_name(name, row, col)
                redwrap.delete_block(block_name)
        self.redis_master.delete(info_key)
    
    def __init_redis(self, config):
        # Reset client ids
        self.redis_master.set('client_id', 0)
        # Clear the list of free jobs
        self.redis_master.delete(const.FREE_JOBS_KEY.format(self.name))
        self.redis_master.delete(const.SLAVE_LIST_KEY.format(self.name))
        
    def __create_key_manager(self, config):
        slaves = []
        for k,v in config['redis_slaves'].items():
            self.redis_master.set(const.SLAVE_KEY.format(self.name, k), json.dumps(v))
            self.redis_master.lpush(const.SLAVE_LIST_KEY.format(self.name), k)
            slaves.append(k)
        return KeyManager(slaves, self.name)