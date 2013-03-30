from redisml.server import jobs

class FailJob(jobs.Job)

    def __init__(self, redis):
        super(FailJob, self).__init__(redis)
        
    def run(self):
        self.add_subjob('FAIL')
        self.execute()
        
class TimeoutJob(jobs.Job):
    def __init__(self, redis):
        super(FailJob, self).__init__(redis)
        self.time = 10
        
    def setTime(self, time):
        self.time = time
                
    def run(self):
        self.add_subjob('TIMEOUT ' + str(self.time))
        self.execute()