import numpy
import time

def mtrans(r, cmdArgs):
	m = numpy.loads(r.get(cmdArgs[0]))
	res = m.transpose()
	r.set(cmdArgs[1], res.dumps())

def mmult(r, cmdArgs):
	if len(cmdArgs) < 3:
		raise Exception('Too few arguments')
		
	print 'Multiplying blocks ' + cmdArgs[0] + " & " + cmdArgs[1] + ' = ' + cmdArgs[2]
	m1 = numpy.loads(r.get(cmdArgs[0]))
	m2 = numpy.loads(r.get(cmdArgs[1]))
	m3 = m1.dot(m2)
	r.set(cmdArgs[2], m3.dumps())
	
def madd(r, cmdArgs):
	if len(cmdArgs) < 3:
		raise Exception('Too few arguments')
	
	s = 'Adding blocks'
	for i in cmdArgs[0:len(cmdArgs)-1]:
		s += ' ' + i
	s += ' = ' + cmdArgs[len(cmdArgs)-1]
	print s
	
	m = numpy.loads(r.get(cmdArgs[0]))
	for i in cmdArgs[1:len(cmdArgs)-1]:
		m += numpy.loads(r.get(i))
	
	print 'Result has shape ' + str(m.shape)
	r.set(cmdArgs[len(cmdArgs)-1], m.dumps())

def msmult(r, cmdArgs):
	scalar = float(cmdArgs[0])
	matrix = numpy.loads(r.get(cmdArgs[1]))
	result = scalar * matrix
	r.set(cmdArgs[2], result.dumps())
	
def mrand(r, cmdArgs):
	pass
