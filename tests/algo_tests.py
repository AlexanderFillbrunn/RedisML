import unittest
import redisml.server.server as server
import redisml.server.configuration as config
import os
import numpy
import numpy.testing

class IOTestCase(unittest.TestCase):
    
    def setUp(self):
        cfg = config.load_config('config.cfg')
        self.server = server.Server(cfg)
               
    def testKMeans(self):
        m_ = numpy.matrix([ [0,0],
                            [1,0],
                            [0,1],
                            [1,1],
                            [5,5],
                            [5,6],
                            [6,5],
                            [6,6]])
        c_ = m_[:2,:]
        m = self.server.matrix_from_numpy(m_)
        c = self.server.matrix_from_numpy(c_)
        c_old = None
        
        while c_old == None or not c.equals(c_old):
            dist = m.k_means_distance(c)
            c_old = c
            c = m.k_means_recalc(dist)
        
        numpy.testing.assert_array_almost_equal(numpy.matrix([[0.5, 0.5],[5.5, 5.5]]), c.get_numpy_matrix())
        
if __name__ == '__main__':
    unittest.main(verbosity=2)