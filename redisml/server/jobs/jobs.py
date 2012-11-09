import json
from sets import Set

class Subjob:
	def __init__(self, id, cmd, r_con):
		self.id = id
		self.cmd = cmd
		self.done = False
		self.r_con = r_con
	
	def execute(self):
		self.r_con.add_job(self.cmd, self.id)

class Job:
	def __init__(self, r_con):
		self.r_con = r_con
		self.id = r_con.get_job_id()
		self.subjobs = []
		self.subjob_cnt = 0
		self.done = False

	def add_subjob(self, command):
		s = Subjob(str(self.id) + '.' + str(self.subjob_cnt), command, self.r_con)
		self.subjob_cnt += 1
		self.subjobs.append(s)
		return s
		
	def execute(self):
		pubsub = self.r_con.get_channel_listener()
		pubsub.subscribe('c_results')
		returned_jobs = Set()
		
		for s in self.subjobs:
			s.execute()

		for jmsg in pubsub.listen():
			msg = json.loads(jmsg['data'])
			if not msg['success']:
				pubsub.unsubscribe()
				# TODO: Exception maybe?
				return False
			id = msg['id'].split('.')
			major = int(id[0])
			minor = int(id[1])
			if major == self.id:
				returned_jobs.add(minor)
			if len(returned_jobs) == self.num_subjobs():
				break
		return True
		
	def num_subjobs(self):
		return len(self.subjobs)