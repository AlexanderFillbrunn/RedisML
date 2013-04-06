from redisml.shared import redis_constants as const

class KeyManager:
    
    def __init__(self, slaves, server_name):
        self.__slaves = sorted(slaves)
        self.server_name = server_name
        
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
        
    def get_free_jobs_key(self):
        return const.FREE_JOBS_KEY.format(self.server_name)
        
    def get_results_channel_name(self):
        return const.JOB_RESULTS_CHANNEL.format(self.server_name)
        
    def get_job_ids_key(self):
        return const.JOB_IDS_KEY.format(self.server_name)