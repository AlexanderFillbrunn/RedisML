import numpy
import glob
from redis import Redis

def load_ijv(name, width, height, block_size, host='localhost', port=6379, db=0):
    files = glob.glob('./' + name + '*.ijv')
    red = Redis(host, port=port, db=db)
    file_format = '{0}_{1}_{2}.ijv'
    for row in range(0, height / block_size):
        for col in range(0, width / block_size):
            file = file.open(file_format.format(name, row, col))
            