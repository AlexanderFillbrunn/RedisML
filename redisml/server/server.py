import matrix
from redis import Redis

class Server:
    def __init__(self, config):
        self.redis = Redis(config['host'], port=config['port'], db=config['db'])
        self.matrix_factory = matrix.MatrixFactory(self.redis, config['block_size'])
        
    def matrix_from_numpy(self, mat, name=''):
        return self.matrix_factory.matrix_from_numpy(mat, name)
        
    def matrix_from_name(self, name):
        return self.matrix_factory.matrix_from_name(name)