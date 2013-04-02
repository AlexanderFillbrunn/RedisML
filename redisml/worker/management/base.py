import redis
from redisml.worker import command_handler
import json
import logging

class Worker:
    def __init__(self, host, port, db):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.running = False
        self.workerid = 'pyclient' + str(self.redis.incr('client_id'))
        logging.debug('Client ID: ' + self.workerid)
        
    def run(self):
        self.running = True
        stop = False
        
        while not stop:
            #Get new job
            #blpop returns a tupel of key and item, so take index 1
            job_id = self.redis.blpop('free_jobs')[1]
            #print 'New job: ' + str(job_id)
            if job_id != None:
                #Load job info
                job_str = self.redis.get('job:' + str(job_id))
                print job_str
                job = json.loads(job_str)
                logging.info('New job (' + str(job_id) + '): ' + job['cmd'])
                try:
                    command_handler.execute(self.redis, job['cmd'])
                except Exception, e:
                    self.__fail(job_id, str(e))
                    continue
                self.__finish(job_id)
                #print "=================="
    
    def __fail(self, job_id, message):
        self.redis.publish('c_results', json.dumps({'id' : job_id, 'success' : False, 'reason' : 'fail', 'message' : message, 'client' : self.workerid}))
        logging.error('Error in job ' + str(job_id) + ': ' + message)
        
    def __finish(self, job_id):
        self.redis.sadd('finished_jobs', job_id)
        self.redis.publish('c_results', json.dumps({'id' : job_id, 'success' : True, 'client' : self.workerid}))
        logging.info('Finished job ' + str(job_id))
