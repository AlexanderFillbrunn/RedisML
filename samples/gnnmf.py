import numpy
import redisml.server.server as server
import redisml.server.configuration as config

def gnnmf(v, w, h):
    max_iteration = 10
    for i in range(0, max_iteration):
        # Numpy code:
        # h = h*(w.transpose().dot(h)/w.transpose().dot(w).dot(h))
        # w = w*(v.dot(h.transpose())/w.dot(h).dot(h.transpose()))
        h = h.cw_multiply(w.multiply(h, transpose_self=True).cw_divide(w.multiply(w, transpose_self=True).multiply(h)))
        w = w.cw_multiply(v.multiply(h, transpose_m=True).cw_divide(w.multiply(h).multiply(h, transpose_m=True)))

cfg = config.load_config('config.cfg')
s = server.Server(cfg)

# Here we generate random matrices
# Use Server.matrix_from_name to load an existing matrix from redis
v_ = numpy.random.rand(1024,1024)
v = s.matrix_from_numpy(v_)
w_ = numpy.random.rand(1024,1024)
w = s.matrix_from_numpy(w_)
h_ = numpy.random.rand(1024,1024)
h = s.matrix_from_numpy(h_)

gnnmf(v, w, h)
print h