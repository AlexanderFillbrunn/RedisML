#!/usr/bin/env python -B
import redis
from redisml.server.jobs import jobs
import redisml.server.matrix as matrix
from redisml.server import redis_wrapper as rw
import numpy

r = redis.Redis(host='localhost', port=6379, db=0)
rwrapper = rw.RedisWrapper(r)

m = numpy.random.rand(1024,1024)
mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, rwrapper)

n = numpy.random.rand(1024,1024)
mat2 = matrix.Matrix.from_numpy(n, 'Test2', 256, rwrapper)


mat1.multiply(mat2, 'Test3').print_blocks()
