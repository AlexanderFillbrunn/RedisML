import json, threading, logging
from sets import Set
from redisml.server import settings, exceptions
from redisml.shared import events

class Subjob(object):
    def __init__(self, id, cmd, redis, free_jobs_key):
        self.id = id
        self.cmd = cmd
        self.redis = redis
        self.running = False
        self.currentParent = None
        self.timer = None
        self.timeout = settings.DEFAULT_JOB_TIMEOUT
        self.timeoutOccurred = events.EventHook()
        self.free_jobs_key = free_jobs_key

    def getFullID(self):
        return str(self.currentParent) + '.' + str(self.id)
    
    def getRedisKey(self):
        return 'job:' + str(self.getFullID())
    
    def __timeout(self):
        self.timer = None
        self.timeoutOccurred.fire(self)
    
    def reexecute(self):
        self.execute(self.currentParent)
    
    def setTimeout(self, to):
        self.timeout = to
        if self.running:
            if self.timer != None:
                self.timer.cancel()
            self.timer = threading.Timer(self.timeout, self.__timeout)
            self.timer.start()
        
    def execute(self, parentID):
        if self.running:
            raise Exception('Job is still running. Call finish to stop it.')
        self.currentParent = parentID
        # Add job to redis
        job = json.dumps({'id' : self.getFullID(), 'cmd' : self.cmd})
        self.redis.set(self.getRedisKey(), job)
        self.redis.rpush(self.free_jobs_key, self.getFullID())
        self.running = True
        if self.timeout > 0:
            self.timer = threading.Timer(self.timeout, self.__timeout)
            self.timer.start()

    def finish(self):
        self.redis.delete(self.getRedisKey())
        self.running = False
        if self.timer != None:
            self.timer.cancel()
        self.timer = None
    
class Job(object):
    def __init__(self, redis, key_mngr):
        self.redis = redis
        self.subjobs = []
        self.subjobTimeout = settings.DEFAULT_JOB_TIMEOUT
        # Get keys
        self.results_channel_name = key_mngr.get_results_channel_name()
        self.free_jobs_key = key_mngr.get_free_jobs_key()
        self.job_ids_key = key_mngr.get_job_ids_key()

    def add_subjob(self, command):
        s = Subjob(len(self.subjobs), command, self.redis, self.free_jobs_key)
        s.setTimeout(self.subjobTimeout)
        s.timeoutOccurred += self.subjobTimeoutOccurred
        self.subjobs.append(s)
        return s
    
    def setSubjobTimeout(self, to):
        self.subjobTimeout = to
        for s in self.subjobs:
            s.setTimeout(self.subjobTimeout)
    
    def __getLogger(self, jobid):
        logger = logging.getLogger('jobs').getChild('job' + str(jobid))
        return logger
    
    def subjobTimeoutOccurred(self, sender):
        self.redis.publish(self.results_channel_name, json.dumps({'id' : sender.getFullID(), 'success' : False, 'reason' : 'timeout', 'message' : 'Timeout after ' + str(sender.timeout) + ' seconds' }))
    
    def execute(self):
        # For each execution a new job id is assigned so a job can safely be executed multiple times
        jobid = self.redis.incr(self.job_ids_key)
        logger = self.__getLogger(jobid)
        # Register pubsub for listening for job results
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.results_channel_name)
        
        self.returned_jobs = Set()
        self.jobcount = []

        # Execute all subjobs once
        for s in self.subjobs:
            self.jobcount.append(1)
            s.execute(jobid)

        while len(self.returned_jobs) < self.num_subjobs():
            for jmsg in pubsub.listen():
                # Skip the published message if it has the wrong type
                if jmsg['type'] != 'message':
                    continue
                # Parse the message and get the job id
                msg = json.loads(jmsg['data'])
                id = msg['id'].split('.')
                major = int(id[0])
                minor = int(id[1])
                # skip message if message is not about this job
                if major != jobid:
                    continue
                # Tell the subjob that a result has been received and a timeout cannot occur anymore
                self.subjobs[minor].finish()
                # Check if subjob failed
                if msg['success']:
                    self.returned_jobs.add(minor)
                    logger.info('Subjob ' + msg['id'] + ' successfully executed by client ' + msg['client'])
                else:
                    count = self.jobcount[minor]
                    if count < settings.MAX_JOB_EXECUTIONS or settings.MAX_JOB_EXECUTIONS == 0:

                        logger.warn('Subjob {0} failed. Reason: {1}. Try: {2}'.format(msg['id'], msg['reason'], str(self.jobcount[minor])))
                        
                        # Try again
                        self.subjobs[minor].reexecute()
                        self.jobcount[minor] += 1
                    else:
                        # Number of executions exceeded the maximum, raise exception
                        pubsub.unsubscribe()
                        logger.warn('Subjob {0} ultimately failed. Reason: {1}. Try: {2}'.format(msg['id'], msg['reason'], str(self.jobcount[minor])))
                        raise exceptions.JobException('Job failed ' + str(count) + ' times. Last error: ' + msg['message'])
                    
                if len(self.returned_jobs) == self.num_subjobs():
                    break
        return True
        
    def num_subjobs(self):
        return len(self.subjobs)