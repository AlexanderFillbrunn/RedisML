
def build_command(name, *params):
	#Simple method for building commands
	output = name 
	for p in params:
		output += ' ' + p 
	return output
	
class CommandBuilder:
	'''
	Builds commands for sending to clients
	'''
	def __init__(self, cmd_name):
		self.cmd_name = cmd_name
		self.params = []
		
	def add_param(self, param):
		''' Adds a parameter to the command '''
		self.params.append(' ' + param)
	
	def getCmdString(self):
		''' Returns the built command as a string '''
		return self.cmd_name + ' '.join(self.params)
	
	def join(self, cb):
		''' Joins the command with another one and returns the string '''
		return self.getCmdString() + '\n' + cb.getCmdString()
	
	def __str__(self):
		return self.getCmdString()
		