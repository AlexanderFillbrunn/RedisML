import numpy
import glob
from redis import Redis
import sys

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

