from redisml.shared import redis_constants as const

class KeyManager:
    
    def __init__(self, slaves):
        self.__slaves = sorted(slaves)
        
    def get_slave(self, *num):
        if len(self.__slaves) == 1:
            return self.__slaves[0]
        else:
            return self.__slaves[sum(num) % len(self.__slaves)]
        
        
    def get_block_name(self, matrix_name, row, col):
        slave_name = self.__slaves[0]
        if len(self.__slaves) > 1:
            slave_name = self.__slaves[(row+col) % len(self.__slaves)]
        
        return const.get_block_name(matrix_name, slave_name, row, col)