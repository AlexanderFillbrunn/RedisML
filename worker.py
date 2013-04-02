from redisml.worker.management.base import Worker
from redisml.worker import worker_settings
import redis

"""
import redis
from client import command_handler
import json

r = redis.Redis(host='localhost', port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe('c_jobs')
stop = False

while not stop:
    job_id = r.spop('free_jobs')

    if job_id != None:
        job = json.loads(r.get('job:' + str(job_id)))
        
        try:
            command_handler.execute(r, job['cmd'])
        except Exception, e:
            print 'Error: ' + str(e)
            r.publish('c_results', json.dumps({'id' : job_id, 'success' : False, 'reason' : 'fail'}))
            
        r.sadd('finished_jobs', job_id)
        r.publish('c_results', json.dumps({'id' : job_id, 'success' : True}))
    else:
        #Block until a new job gets published
        print 'No jobs, listening...'
        for j in pubsub.listen():
            data = j['data'].split(' ')
            if data[0] == 'JOB':
                if not r.sismember('finished_jobs', data[1]):
                    break
"""

worker_settings.initLogging()
c = Worker('localhost', 6379, 0)
c.run()