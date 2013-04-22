import numpy as np
import redisml.server.server as server
import redisml.server.configuration as config
import numpy.testing

cfg = config.load_config('config.cfg')
s = server.Server(cfg)

m_ = np.random.rand(512,512)
m = s.matrix_from_numpy(m_)

res = m.slice(0, 512, 257, 255)
r1 = res.get_numpy_matrix()
r2 = m_[0:512,257:512]

print m_
print r1
np.testing.assert_array_almost_equal(r1, r2)