import redis
import threading
import time
import json
from redisml.shared import pubsub_helper, notify

running_jobs = {}
stop_checking = False
TIMEOUT = 600

class ClientObserver(notify.Observer):
	def __init__(self, red):
		notify.Observer.__init__(self)
		self.red = red
		
	def notify_success(self, sender, args):
		data = args.data[1]
		channel = args.data[0]
		if channel == 'c_results':
			running_jobs.remove(data)
		else:
			running_jobs[data] = time.time()
		print '- ' + args.data[0] + ': ' + args.data[1]
		
	def notify_error(self, sender, args):
		print 'error'

def check_time():
	while not stop_checking:
		elapsed = []
		for k,v in running_jobs:
			if time.time() - v > TIMEOUT:
				r.publish('c_results', json.dumps({'id' : k, 'success' : False, 'reason' : 'timeout'}))
				elapsed.append(k)
		for j in elapsed:
			del running_jobs[j]
		time.sleep(1)

r = redis.Redis(host='localhost', port=6379, db=0)
o = ClientObserver(r)
l = psh.ChannelListener(['c_jobs', 'c_results'], r)
l.add_observer(o)
l.start()

t = threading.Thread(target = check_time)
t.setDaemon(True)
t.start()