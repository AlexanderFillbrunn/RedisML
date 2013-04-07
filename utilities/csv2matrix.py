import sys
import redisml.shared.io.fileio as io

input = sys.argv[1]
out_prefix = sys.argv[2]
block_size = int(sys.argv[3])
separator = ';'
if len(sys.argv) > 4:
    separator = sys.argv[4]

io.transform(input, out_prefix, block_size, separator)