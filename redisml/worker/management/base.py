import redis
from redisml.worker import command_handler
import redisml.shared.redis_constants as const
import json
import logging

class Worker:
    def __init__(self, server_name, host, port, db):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.running = False
        self.workerid = 'pyclient' + str(self.redis.incr('client_id'))
        logging.info('Client ID: ' + self.workerid)
        
        self.server_name = server_name
        # Keys
        self.free_jobs_key = const.FREE_JOBS_KEY.format(server_name)
        self.finished_jobs_key = const.FINISHED_JOBS_KEY.format(server_name)
        self.results_channel_name = const.JOB_RESULTS_CHANNEL.format(server_name)
        
    def run(self):
        self.running = True
        stop = False
        slaves = self.redis.lrange(const.SLAVE_LIST_KEY.format(self.server_name), 0, -1)
        
        while not stop:
            #Get new job
            #blpop returns a tupel of key and item, so take index 1
            job_id = self.redis.blpop(self.free_jobs_key)[1]
            #print 'New job: ' + str(job_id)
            if job_id != None:
                #Load job info
                job_str = self.redis.get('job:' + str(job_id))
                # print job_str
                job = json.loads(job_str)
                # logging.info('New job (' + str(job_id) + '): ' + job['cmd'])
                #try:
                command_handler.execute(self.redis, self.server_name, slaves, job['cmd'])
                #except Exception, e:
                #    self.__fail(job_id, str(e))
                #    continue
                self.__finish(job_id)
    
    def __fail(self, job_id, message):
        self.redis.publish(self.results_channel_name,
            json.dumps({'id' : job_id, 'success' : False, 'reason' : 'fail', 'message' : message, 'client' : self.workerid}))
        logging.error('Error in job ' + str(job_id) + ': ' + message)
        
    def __finish(self, job_id):
        self.redis.sadd(self.finished_jobs_key, job_id)
        self.redis.publish(self.results_channel_name, json.dumps({'id' : job_id, 'success' : True, 'client' : self.workerid}))
        # logging.info('Finished job ' + str(job_id))
