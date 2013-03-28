import redis
import numpy
import json
import redisml.shared.matrix_serialization as ser

class RedisWrapper:
	def __init__(self, red):
		self.redis = red
	
	def get_job_id(self):
		return self.redis.incr('job_ids')
	
	def get_job_ids(self, num):
		p = self.redis.pipeline()
		for i in range(0,num):
			p.incr('job_ids')	
		return p.execute()
		
	def add_job(self, command, id):
		job = json.dumps({'id' : id, 'cmd' : command})
		#self.redis.sadd('free_jobs', id)
		self.redis.set('job:' + str(id), job)
		#self.redis.publish('c_jobs', 'JOB ' + id)
		self.redis.rpush('free_jobs', id)
	
	def fail_job(self, id):
		self.redis.publish('c_jobs', 'FAIL ' + str(id))
	
	def get_channel_listener(self):
		return self.redis.pubsub()
		
	def create_block(self, block_name, data):
		self.redis.set(block_name, data.dumps())
		
	def get_block(self, key):
		return numpy.loads(self.redis.get(key))