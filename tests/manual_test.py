import redis
import numpy
import numpy.testing
from math import *
from redisml.server.jobs import jobs
import redisml.server.matrix as matrix
from redisml.server import redis_wrapper as rw

#try:
r = redis.Redis(host='localhost', port=6379, db=0)
r.flushdb()

fac = matrix.MatrixFactory(r, 2)

m = numpy.matrix([[1, 1, 3, 3],
				  [2, 2 ,4, 4],
				  [2, 1 ,4, 5],
				  [2, 1 ,4, 7]])
				  
n = numpy.matrix([[1, 1, 3, 3],
				  [2, 2 ,4, 4],
				  [2, 1 ,4, 5],
				  [2, 1 ,4, 7]])
				  
mat1 = fac.matrix_from_numpy(m)
mat2 = fac.matrix_from_numpy(n)
res = mat1.cw_add(mat2)
print res
#except Exception as e:
#	print str(e)