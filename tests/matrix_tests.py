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
    
    def testTrace(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        res = mat1.trace()
        self.assertAlmostEqual(res, m.trace())
        
    def testScalarMult(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        result = mat1.scalar_multiply(5)
        res = 5 * m
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on scalar multiplication')

    def testTranspose(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        res = mat1.transpose()
        
        numpy.testing.assert_array_almost_equal(res.get_numpy_matrix(), m.T, err_msg='Numpy and RedisML produce different results on transpose')

    def testColSums(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        result = mat1.col_sums()
        res = m.sum(axis=0)
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix()[0,:], err_msg='Numpy and RedisML produce different results on colsums')
    
    def testSpecialColSums(self):
        m = numpy.arange(9).reshape(3,3)
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        result = mat1.col_sums(expr='x+1')
        res = m.sum(axis=0) + 3
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix()[0,:], err_msg='Numpy and RedisML produce different results on colsums')
    
    def testRowSums(self):
        m = numpy.arange(12).reshape(3,4)
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        result = mat1.row_sums()
        res = m.sum(axis=1)
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix()[:,0], err_msg='Numpy and RedisML produce different results on colsums')

    def testCellAccess(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        self.assertEqual(mat1.get_cell_value(500,500), m[500,500])

    def testSimpleMatrixCreation(self):
        m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
        
    def testMatrixCreation(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        numpy.testing.assert_array_equal(m, mat1.get_numpy_matrix())
    
    def testTransposeNegateMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        n = numpy.random.rand(1024,1024)
        mat2 = matrix.Matrix.from_numpy(n, 256, self.rwrapper)
        
        result1 = mat1.multiply(mat2, transpose_self=True, negate_m=True)
        result2 = m.transpose().dot(-n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')

        
    def testNegateTransposeMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        n = numpy.random.rand(1024,1024)
        mat2 = matrix.Matrix.from_numpy(n, 256, self.rwrapper)
        
        result1 = mat1.multiply(mat2, transpose_m=True, negate_self=True)
        result2 = (-m).dot(n.transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')
        
    def testFullModifierMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        n = numpy.random.rand(1024,1024)
        mat2 = matrix.Matrix.from_numpy(n, 256, self.rwrapper)
        
        result1 = mat1.multiply(mat2, transpose_m=True, negate_self=True, transpose_self=True, negate_m=True)
        result2 = (-m).transpose().dot((-n).transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')
    
    def testSimpleMultiply(self):
        m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
        mat1 = matrix.Matrix.from_numpy(m, 2, self.rwrapper)
        n = numpy.matrix([[11,12,13],[14,15,16],[17,18,19]])
        mat2 = matrix.Matrix.from_numpy(n, 2, self.rwrapper)
        
        result1 = mat1.multiply(mat2)
        result2 = m.dot(n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2,
                                                    err_msg='Numpy and RedisML produce different results')

    def testMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        n = numpy.random.rand(1024,1024)
        mat2 = matrix.Matrix.from_numpy(n, 256, self.rwrapper)
        
        result1 = mat1.multiply(mat2)
        result2 = m.dot(n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results')
        mat1.delete()
        mat2.delete()
        result1.delete()
        
    def testAdd(self):
        m = numpy.random.rand(1024,1024)
        mat1 = matrix.Matrix.from_numpy(m, 256, self.rwrapper)
        n = numpy.random.rand(1024,1024)
        mat2 = matrix.Matrix.from_numpy(n, 256, self.rwrapper)
        result1 = mat1.cw_add(mat2)
        result2 = m + n
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2, err_msg='Numpy and RedisML produce different results on addition')
'''
    def testMatrixFactorization(self):
        v = numpy.random.rand(1024,1024)
        _v = matrix.Matrix.from_numpy(v, 256, self.rwrapper)
        w = numpy.random.rand(1024,1024)
        _w = matrix.Matrix.from_numpy(w, 256, self.rwrapper)
        h = numpy.random.rand(1024,1024)
        _h = matrix.Matrix.from_numpy(h, 256, self.rwrapper)

        max_iteration = 10
        for i in range(0, max_iteration):
            h = h*(w.transpose().dot(h)/w.transpose().dot(w).dot(h))
            w = w*(v.dot(h.transpose())/w.dot(h).dot(h.transpose()))
            _h = _h.cw_multiply(_w.multiply(_h, transpose_self=True).cw_divide(_w.multiply(_w, transpose_self=True).multiply(_h)))
            _w = _w.cw_multiply(_v.multiply(_h, transpose_m=True).cw_divide(_w.multiply(_h).multiply(_h, transpose_m=True)))
            
        numpy.testing.assert_array_almost_equal(_h.get_numpy_matrix(), h, err_msg='Numpy and RedisML produce different results')
        numpy.testing.assert_array_almost_equal(_w.get_numpy_matrix(), w, err_msg='Numpy and RedisML produce different results')
        
        _h.delete()
        _w.delete()
        _v.delete()
'''
if __name__ == '__main__':
    unittest.main()