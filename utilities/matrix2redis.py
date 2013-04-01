import numpy
import glob
from redis import Redis
from redisml.server.matrix import Matrix
import ijv_helpers
import sys

def load_ijv(name, rows, cols, host='localhost', port=6379, db=0):
    red = Redis(host, port=port, db=db)
    file_format = '{0}_{1}_{2}.ijv'
    matrix_format = '{0}_{1}_{2}'

    file = file.open(file_format.format(name, 0, 0))
    matrix = ijv_helpers.load_ijv(file)
    red.set(matrix_format.format(name, 0, 0), matrix.dumps())
    block_size = matrix.shape[0]
    
    red.hmset(Matrix.INFO_FORMAT.format(name), { 'block_size': block_size, 'rows' : rows, 'cols' : cols })
    
    for row in range(0, rows / block_size):
        for col in range(0, cols / block_size):
            if not row == col and row == 0:
                file = file.open(file_format.format(name, row, col))
                matrix = ijv_helpers.load_ijv(file)
                red.set(matrix_format.format(name, row, col), matrix.dumps())
            
def load(name, rows, cols, host='localhost', port=6379, db=0):
    red = Redis(host, port=port, db=db)
    file_format = '{0}_{1}_{2}.blc'
    matrix_format = '{0}_{1}_{2}'
    
    file = file_format.format(name, 0, 0)
    matrix = numpy.genfromtxt(file ,delimiter=';',dtype=None)
    red.set(matrix_format.format(name, 0, 0), matrix.dumps())
    block_size = matrix.shape[0]

    red.hmset(Matrix.INFO_FORMAT.format(name), { 'block_size': block_size, 'rows' : rows, 'cols' : cols })
    
    for row in range(0, rows / block_size):
        for col in range(0, cols / block_size):
            if not (row == col and row == 0):
                file = file_format.format(name, row, col)
                matrix = numpy.genfromtxt(file ,delimiter=';',dtype=None)
                red.set(matrix_format.format(name, row, col), matrix.dumps())
            
def save(name, outname, host='localhost', port=6379, db=0):
    red = Redis(host, port=port, db=db)
    file_format = '{0}_{1}_{2}.blc'
    matrix_format = '{0}_{1}_{2}'
    info = red.hgetall(Matrix.INFO_FORMAT.format(name))
    print info
    
    rows = int(info['rows'])
    cols = int(info['cols'])
    block_size = int(info['block_size'])
    for row in range(0, rows / block_size):
        for col in range(0, cols / block_size):
            file = file_format.format(outname, row, col)
            matrix = numpy.loads(red.get(matrix_format.format(name, row, col)))
            numpy.savetxt(file, matrix, delimiter=';', fmt='%f')

arg_end = 3           
mode = sys.argv[1]
name = sys.argv[2]
outname = None
host = 'localhost'
port = 6379
db = 0
rows = None
cols = None

if mode == 'load' or mode == 'loadijv':
    rows = int(sys.argv[3])
    cols = int(sys.argv[4])
    arg_end = 5
elif mode == 'save':
    outname = sys.argv[3]
    arg_end = 4

if len(sys.argv) > arg_end:
    host = sys.argv[arg_end]
    arg_end += 1

if len(sys.argv) > arg_end:
    port = int(sys.argv[arg_end])
    arg_end += 1
    
if len(sys.argv) > arg_end:
    db = int(sys.argv[arg_end])

if mode == 'load':
    load(name, rows, cols, host=host, port=port, db=db)
elif mode == 'save':
    save(name, outname, host=host, port=port, db=db)

