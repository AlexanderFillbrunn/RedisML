import numpy
import redisml.server.server as server
import redisml.server.configuration as config

def kmeans(data, centers, k):
    # iterate k-means
    for i in range(0, k):
        dist = data.k_means_distance(centers)
        centers = data.k_means_recalc(dist)


# Create a server
cfg = config.load_config('config.cfg')
s = server.Server(cfg)

# Generate random data
data_ = numpy.random.rand(1024, 16)
# Take first 10 rows as centers
centers_ = data_[0:10,:]

# Create redisml matrices
data = s.matrix_from_numpy(data_)
centers = s.matrix_from_numpy(centers_)

kmeans(data, centers, 10)
    
print centers