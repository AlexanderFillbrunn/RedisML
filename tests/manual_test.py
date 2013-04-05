import numpy
import redisml.server.server as server
import redisml.server.configuration as config

cfg = config.load_config('config.cfg')
cfg['matrix']['block_size'] = 2
s = server.Server(cfg)


m = numpy.arange(12).reshape(3,4)
mat1 = s.matrix_from_numpy(m)
result = mat1.col_sums()
res = m.sum(axis=0)
numpy.testing.assert_array_almost_equal(res, result.get_numpy_matrix(), err_msg='Numpy and RedisML produce different results on colsums')