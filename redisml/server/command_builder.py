
def build_command(name, *params):
	#Simple method for building commands
	output = name 
	for p in params:
		output += ' ' + p 
	return output
	
class CommandBuilder:
	"""
	Builds commands for sending to clients
	"""
	def __init__(self, cmd_name):
		self.cmd_name = cmd_name
		self.params = []
		
	def add_param(self, param):
		self.params.append(' ' + param)
	
	def getCmdString(self):
		return self.cmd_name + ' '.join(self.params)
	
	def __str__(self):
		return self.getCmdString()
		