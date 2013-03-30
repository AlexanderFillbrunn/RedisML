import sys

def transform_ijv(input_file, output_prefix, block_size, separator):
    output_files = {}
    output_format = output_prefix + '_{0}_{1}.ijv'
    line_format = '{0};{1};{2}'
    with open(input_file, 'rU') as f:
        for line in f:
            split = line.split(separator)
            row = int(split[0])
            col = int(split[1])
            val = split[2]
            
            block_row = row / block_size
            block_col = col / block_size
            row_offset = row % block_size
            col_offset = col % block_size
            
            file_name = output_format.format(block_row, block_col)
            outfile = None
            if not output_files.has_key(file_name):
                outfile = open(file_name, 'w+')
                output_files[file_name] = outfile
            else:
                outfile = output_files[file_name]
                
            outfile.write(line_format.format(row_offset, col_offset, val))
    for k, v in output_files.items():
        v.close()
        
def transform(input_file, output_prefix, block_size, separator):
    output_files = {}
    output_format = output_prefix + '_{0}_{1}.blc'
    with open(input_file, 'rU') as f:
        row = 0
        for line in f:
            block_start = 0
            count = 0
            col = 0
            if not line[-1] == '\n':
                line = line + '\n'
            for char in line:
                if char == separator or char == '\n':
                    col += 1
                    if (col % block_size == 0 or char == '\n') and count != 0 and block_start != count:
                        d = line[block_start:count]
                        row_block = row / block_size
                        col_block = (col-1) / block_size
                        
                        file_name = output_format.format(row_block, col_block)
                        outfile = None
                        if not output_files.has_key(file_name):
                            outfile = open(file_name, 'w+')
                            output_files[file_name] = outfile
                        else:
                            outfile = output_files[file_name]
                        outfile.write(d + '\n')

                        block_start = count+1
                count += 1
            row += 1
    for k, v in output_files.items():
        v.close()

input = sys.argv[1]
out_prefix = sys.argv[2]
block_size = int(sys.argv[3])
format = sys.argv[4]
separator = ';'
if len(sys.argv) > 5:
    separator = sys.argv[5]

if format == 'ijv':
    transform_ijv(input, out_prefix, block_size, separator)
elif format == 'mat':
    transform(input, out_prefix, block_size, separator)
    
'''
Based on the comments by jorgeca, the loading and saving of the format is phased out in scipy's [Matrix Market](http://math.nist.gov/MatrixMarket/formats.html#coord).
Doing it manually seems good enough for small matrices.
To load such files into a Numpy matrix, the following code can be used (example for a 3 by 3 matrix):

    import numpy as np
    m = np.empty((3,3))
    file = open("mymatrix.csv")
    for line in file:
        data = line.split(';')
        row = int(data[0])
        col = int(data[1])
        m[row,col] = float(data[2])

The disadvantage of this approach is that the matrix dimensions must be known beforehand, if they are not somehow saved in the matrix file.
'''