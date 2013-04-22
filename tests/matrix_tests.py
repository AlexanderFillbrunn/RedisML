import unittest
import redis
import numpy
import numpy.testing
import logging
import sys
from numpy import linalg as la
import redisml.server.matrix as matrix
import redisml.server.server as server
import redisml.server.configuration as config

class MatrixTestCase(unittest.TestCase):
    
    def setUp(self):
        # initialize the logger
        job_logger = logging.getLogger('jobs')
        job_logger.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        job_logger.addHandler(ch)
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
        result = mat1 * 5
        res = 5 * m
        numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix())

    def testTranspose(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        res = mat1.transpose()
        
        numpy.testing.assert_array_almost_equal(res.get_numpy_matrix(), m.T)

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
        
        result1 = mat1.dot(mat2, transpose_self=True, negate_m=True)
        result2 = m.transpose().dot(-n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)
        
    def testNegateTransposeMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.dot(mat2, transpose_m=True, negate_self=True)
        result2 = (-m).dot(n.transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)
        
    def testFullModifierMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.dot(mat2, transpose_m=True, negate_self=True, transpose_self=True, negate_m=True)
        result2 = (-m).transpose().dot((-n).transpose())
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)
    
    def testSimpleMultiply(self):
        m = numpy.matrix([[1,2,3],[4,5,6],[7,8,9]])
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.matrix([[11,12,13],[14,15,16],[17,18,19]])
        mat2 = self.server.matrix_from_numpy(n)
        
        result1 = mat1.dot(mat2)
        result2 = m.dot(n)
        
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)

    def testMultiply(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        result1 = mat1.dot(mat2)
        result2 = m.dot(n)
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)
    
    def testSum(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        self.assertAlmostEqual(m.sum(), mat1.sum())
    
    def testCellwiseMultiplication(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)       
        res = m * n
        result = mat1 * mat2      
        numpy.testing.assert_array_almost_equal(result.get_numpy_matrix(), res)
    
    def testCellwiseAdd(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        n = numpy.random.rand(1024,1024)
        mat2 = self.server.matrix_from_numpy(n)
        result1 = mat1.cw_add(mat2)
        result2 = m + n
        numpy.testing.assert_array_almost_equal(result1.get_numpy_matrix(), result2)

    def testVectorNorm(self):
        m = numpy.random.rand(1024,1)
        mat1 = self.server.matrix_from_numpy(m)
        self.assertAlmostEqual(la.norm(m, 2), mat1.vector_norm(2))
        
    def testMatrixCreation(self):
        m = numpy.ones((900, 900))
        mat1 = self.server.matrix_from_scalar(1, 900, 900)
        numpy.testing.assert_array_almost_equal(mat1.get_numpy_matrix(), m)
        
    def testAggregationRowSum(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.sum(axis=1)
        r2 = numpy.matrix(m.sum(axis=1)).T
        numpy.testing.assert_array_almost_equal(r1.get_numpy_matrix(), r2)
        
    def testAggregationRowMax(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.max(axis=1)
        r2 = numpy.matrix(m.max(axis=1)).T
        numpy.testing.assert_array_almost_equal(r1.get_numpy_matrix(), r2)
        
    def testAggregationColSum(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.sum(axis=0)
        r2 = numpy.matrix(m.sum(axis=0))
        numpy.testing.assert_array_almost_equal(r1.get_numpy_matrix(), r2)
        
    def testAggregationMin(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.min()
        r2 = m.min()
        self.assertAlmostEqual(r1, r2)
        
    def testAggregationColMin(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.min(axis=0)
        r2 = numpy.matrix(m.min(axis=0))
        numpy.testing.assert_array_almost_equal(r1.get_numpy_matrix(), r2)
        
    def testAggregationSpecialColSum(self):
        m = numpy.random.rand(1024,1024)
        mat1 = self.server.matrix_from_numpy(m)
        r1 = mat1.sum(axis=0, expr='numpy.power(x,2)')
        r2 = numpy.matrix(numpy.power(m,2).sum(axis=0))
        numpy.testing.assert_array_almost_equal(r1.get_numpy_matrix(), r2)
       
    def testSlicing(self):
        size = 512
        m = numpy.random.rand(size, size)
        mat1 = self.server.matrix_from_numpy(m)
        
        slice1 = m[0:size,0:size]
        slice2 = mat1.slice(0, size, 0, size).get_numpy_matrix()
        numpy.testing.assert_array_almost_equal(slice1, slice2)
        
        slice1 = m[0:size / 2,0:size / 2]
        slice2 = mat1.slice(0, size/2, 0, size/2).get_numpy_matrix()
        numpy.testing.assert_array_almost_equal(slice1, slice2)
        
        slice1 = m[size / 2:size,size / 2:size]
        slice2 = mat1.slice(size/2, size/2, size/2, size/2).get_numpy_matrix()
        numpy.testing.assert_array_almost_equal(slice1, slice2)
        
        for i in range(0, size-1, 100):
            for j in range(0, size-1, 100):
                slice1 = m[i:size,j:size]
                slice2 = mat1.slice(i, size-i, j, size-j).get_numpy_matrix()
                numpy.testing.assert_array_almost_equal(slice1, slice2)
    
    
if __name__ == '__main__':
    unittest.main(verbosity=2)