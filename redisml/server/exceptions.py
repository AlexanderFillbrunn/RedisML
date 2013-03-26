class MatrixOperationException(Exception):
	def __init__(self, msg, cmd=''):
		super(MatrixOperationException, self).__init__(msg)
		self.cmd = cmd
		self.msg = msg
		
	def __str__(self):
		return 'Error executing command ' + self.cmd + ':\n' + self.msg
		
	def __repr__(self):
		return self.__str__()
		
class JobException(Exception):
	def __init__(self, msg):
		super(JobException, self).__init__(msg)
		self.msg = msg
		
	def __str__(self):
		return self.msg
		
	def __repr__(self):
		return self.__str__()
		
class JobTimeoutExeption(Exception):
	def __init__(self, msg):
		super(JobTimeoutExeption, self).__init__(msg)
		self.msg = msg
		
	def __str__(self):
		return self.msg
		
	def __repr__(self):
		return self.__str__()