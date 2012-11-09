#!/usr/bin/env python -B
import redis
from redisml.server.jobs import jobs
import redisml.server.matrix as matrix
from redisml.server import redis_connection as rc
import numpy

r = redis.Redis(host='localhost', port=6379, db=0)
rwrapper = rc.RedisWrapper(r)

m = numpy.random.rand(1024,1024)#numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, rwrapper)

n = numpy.random.rand(1024,1024)#numpy.matrix([[11,12,13],[14,15,16],[17,18,19]])
mat2 = matrix.Matrix.from_numpy(n, 'Test2', 256, rwrapper)

'''
mat1.print_blocks()
print '==='
mat2.print_blocks()
print '==='
print n*m
'''

mat1.multiply(mat2, 'Test3').print_blocks()

'''
j = jobs.Job(red_c)
for i in range(0,10):
	j.add_subjob('MMULT ABC DEF ' + str(i))
print j.execute()
'''