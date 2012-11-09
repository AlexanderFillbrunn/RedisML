import unittest
import redis
import numpy
import numpy.testing
import redisml.server.matrix as matrix
from redisml.server import redis_wrapper as rw

class MatrixTestCase(unittest.TestCase):
	
	def setUp(self):
		self.redis = redis.Redis(host='localhost', port=6379, db=0)
		self.rwrapper = rw.RedisWrapper(self.redis)

	def tearDown(self):
		#self.redis.flushdb()
		pass
	
	def testTranspose(self):
		m = numpy.random.rand(1024,1024)
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, self.rwrapper)
		numpy.testing.assert_array_almost_equal(mat1.transpose('TestResult').get_numpy_matrix(), m.T, err_msg='Numpy and RedisML produce different results on transpose')


	def testCellAccess(self):
		m = numpy.random.rand(1024,1024)
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, self.rwrapper)
		self.assertEqual(mat1.get_cell_value(500,500), m[500,500])
		mat1.delete()

	def testSimpleMatrixCreation(self):
		m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 2, self.rwrapper)
		numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
		mat1.delete()
		
	def testMatrixCreation(self):
		m = numpy.random.rand(1024,1024)
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, self.rwrapper)
		numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
		mat1.delete()
		
	def testSimpleMultiply(self):
		m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 2, self.rwrapper)
		n = numpy.matrix([[11,12,13],[14,15,16],[17,18,19]])
		mat2 = matrix.Matrix.from_numpy(n, 'Test2', 2, self.rwrapper)
		
		result1 = mat1.multiply(mat2, 'result')
		result2 = m.dot(n)
		
		numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results')
		mat1.delete()
		mat2.delete()
		result1.delete()

	def testMultiply(self):
		m = numpy.random.rand(1024,1024)
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 256, self.rwrapper)
		n = numpy.random.rand(1024,1024)
		mat2 = matrix.Matrix.from_numpy(n, 'Test2', 256, self.rwrapper)
		
		result1 = mat1.multiply(mat2, 'result')
		result2 = m.dot(n)
		
		numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results')
		mat1.delete()
		mat2.delete()
		result1.delete()
	'''
	def testBigMultiply(self):
		m = numpy.random.rand(5000,5000)
		mat1 = matrix.Matrix.from_numpy(m, 'Test1', 1024, self.rwrapper)
		n = numpy.random.rand(5000,5000)
		mat2 = matrix.Matrix.from_numpy(n, 'Test2', 1024, self.rwrapper)
		
		result1 = mat1.multiply(mat2, 'result')
		result2 = m.dot(n)
		
		numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results')
		mat1.delete()
		mat2.delete()
		result1.delete()
	'''
if __name__ == '__main__':
	unittest.main()