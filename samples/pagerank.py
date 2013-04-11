import numpy as np
import redisml.server.server as server
import redisml.server.configuration as config

cfg = config.load_config('config.cfg')
s = server.Server(cfg)

N = 5
d = 0.8
v_quadratic_error = 0.001

v_ = np.random.rand(5,1)
last_v_ = np.ones((5,1)) * 999
M_ = np.matrix([ [0,     0,      0,      0,   1],
                 [0,     0,      1,      0,   0],
                 [0,     0,      0,      0,   0],
                 [0,     1,      0,      0,   0],
                 [1,     0,      0,      1,   0]])
ones_ = np.ones((N, N))

v = s.matrix_from_numpy(v_)
last_v = s.matrix_from_numpy(last_v_)
M = s.matrix_from_numpy(M_)
ones = s.matrix_from_numpy(ones_)

v = v / v.vector_norm(2)

M_hat = (M * d) + ones * ((1 - d) / N)
            
count = 0
while (v - last_v).vector_norm(2) > v_quadratic_error:
    last_v = v
    v = M_hat.multiply(v)
    v = v / v.vector_norm(2)
    count += 1

print count
print v