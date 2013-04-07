
# Divides a matrix file into several block files
def transform(input_file, output_prefix, block_size, separator=';'):
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
