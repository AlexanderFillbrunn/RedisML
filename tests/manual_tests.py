import numpy as np
import redisml.server.server as server
import redisml.server.configuration as config
import numpy.testing

cfg = config.load_config('config.cfg')
s = server.Server(cfg)

m_ = np.random.rand(6,6)
m = s.matrix_from_numpy(m_)

print m_
print "---"
print m[1:2,1:3]
print m[1:5]