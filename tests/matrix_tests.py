import unittest
import redis
import numpy
import numpy.testing
import redisml.server.matrix as matrix
import redisml.server.server as server
import redisml.server.configuration as config

class MatrixTestCase(unittest.TestCase):
    
    def setUp(self):
        cfg = config.load_config('config.cfg')
        self.server = server.Server(cfg)

    def tearDown(self):
        #self.redis.flushdb()
        pass
    
    def testTrace(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        res = mat1.trace()
        self.assertAlmostEqual(res, m.trace())
        
    def testScalarMult(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = self.server.matrix_from_numpy(m)
        result = mat1.scalar_multiply(5)
        res = 5 * m
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on scalar multiplication')

    def testTranspose(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        res = mat1.transpose()
        
        numpy.testing.assert_array_almost_equal(res.get_numpy_matrix(), m.T, err_msg='Numpy and RedisML produce different results on transpose')

    def testColSums(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = self.server.matrix_from_numpy(m)
        result = mat1.col_sums()
        res = numpy.matrix([m.sum(axis=0)])
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on colsums')
    
    def testSpecialColSums(self):
        m = numpy.arange(9).reshape(3,3)
        mat1 = self.server.matrix_from_numpy(m)
        result = mat1.col_sums(expr='x+1')
        res = numpy.matrix([m.sum(axis=0)]) + 3
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on colsums')
    
    def testRowSums(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = self.server.matrix_from_numpy(m)
        result = mat1.row_sums()
        res = numpy.matrix([m.sum(axis=1)])
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on colsums')

    def testCellAccess(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        self.assertEqual(mat1.get_cell_value(500,500), m[500,500])

    def testSimpleMatrixCreation(self):
        m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
        mat1 = self.server.matrix_from_numpy(m)
        numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
        
    def testMatrixCreation(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
    
    def testTransposeNegateMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.multiply(mat2, transpose_self=True, negate_m=True)
        result2 = m.transpose().dot(-n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')

        
    def testNegateTransposeMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.multiply(mat2, transpose_m=True, negate_self=True)
        result2 = (-m).dot(n.transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')
        
    def testFullModifierMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.multiply(mat2, transpose_m=True, negate_self=True, transpose_self=True, negate_m=True)
        result2 = (-m).transpose().dot((-n).transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')
    
    def testSimpleMultiply(self):
        m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.matrix([[11,12,13],[14,15,16],[17,18,19]])
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.multiply(mat2)
        result2 = m.dot(n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')

    def testMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.multiply(mat2)
        result2 = m.dot(n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results')
        mat1.delete()
        mat2.delete()
        result1.delete()
        
    def testAdd(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        result1 = mat1.cw_add(mat2)
        result2 = m + n
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results on addition')

if __name__ == '__main__':
    unittest.main(verbosity=2)