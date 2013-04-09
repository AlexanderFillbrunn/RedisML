import unittest
import redisml.shared.io.fileio as fileio
import redisml.shared.io.redisio as redisio
import redisml.server.server as server
import redisml.server.configuration as config
import os
import numpy

class IOTestCase(unittest.TestCase):
    
    FILE_FORMAT = 'test1_{0}_{1}.blc'
    NEW_FILE_FORMAT = 'test2_{0}_{1}.blc'
    
    def setUp(self):
        cfg = config.load_config('config.cfg')
        self.server = server.Server(cfg)
    
    def tearDown(self):
        for i in range(0, 4):
            for j in range(0, 4):
                file1 = IOTestCase.FILE_FORMAT.format(i, j)
                file2 = IOTestCase.NEW_FILE_FORMAT.format(i, j)
                if os.path.exists(file1):
                    os.remove(file1) 
                if os.path.exists(file2):
                    os.remove(file2)    
        self.server.delete_matrix('testmatrix')
               
    def testMatrixLoading(self):
        fileio.transform('test.csv', IOTestCase.FILE_FORMAT, 256, separator=';')
        redisio.file2redis('testmatrix', 1024, 1024, self.server.redis_master, self.server.key_manager, IOTestCase.FILE_FORMAT, block_size=256)
        redisio.redis2file('testmatrix', self.server.redis_master, self.server.key_manager, IOTestCase.NEW_FILE_FORMAT)
        
        for i in range(0, 4):
            for j in range(0, 4):
                matrix1 = numpy.genfromtxt(IOTestCase.FILE_FORMAT.format(i, j) ,delimiter=';',dtype=None)
                matrix2 = numpy.genfromtxt(IOTestCase.NEW_FILE_FORMAT.format(i, j) ,delimiter=';',dtype=None)
                numpy.testing.assert_array_almost_equal(matrix1, matrix2)   
        
if __name__ == '__main__':
    unittest.main(verbosity=2)