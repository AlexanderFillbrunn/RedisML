import numpy as np
import redisml.server.server as server
import redisml.server.configuration as config

cfg = config.load_config('config.cfg')
s = server.Server(cfg)

N = 5
d = 0.8
v_quadratic_error = 0.001

v_ = np.random.rand(5,1)
M_ = np.matrix([ [0,     0,      0,      0.5,   0.5],
                 [0,     0,      1,      0,   0],
                 [0,     0,      0,      0,   0],
                 [0,     1,      0,      0,   0],
                 [1,     0,      0,      0.5,   0.5]])

v = s.matrix_from_numpy(v_)
last_v = s.matrix_from_scalar(999, 5, 1)
M = s.matrix_from_numpy(M_)
ones = s.matrix_from_scalar(1, N, N)

v = v / v.vector_norm(2)

M_hat = (M * d) + ones * ((1 - d) / N)
            
count = 0
while (v - last_v).vector_norm(2) > v_quadratic_error:
    last_v = v
    v = M_hat.dot(v)
    v = v / v.vector_norm(2)
    count += 1

print count
print v