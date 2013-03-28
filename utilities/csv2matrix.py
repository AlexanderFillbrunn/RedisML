import sys

def transform(input_file, output_prefix, block_size, separator):
	output_files = {}
	output_format = output_prefix + '_{0}_{1}.blc'
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
		
input = sys.argv[1]
out_prefix = sys.argv[2]
block_size = int(sys.argv[3])
separator = ';'
if len(sys.argv) > 4:
	separator = sys.argv[4]
	
transform(input, out_prefix, block_size, separator)