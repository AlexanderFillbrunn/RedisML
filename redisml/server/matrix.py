from jobs import jobs
import redisml.server.command_builder as command_builder
import exceptions
import math
import numpy
import json

#(row, col)
class Matrix:
	def __init__(self, rows, cols, name, block_size, redwrap):
		self.__rows = rows
		self.__cols = cols
		self.__name = name
		self.__redwrap = redwrap
		self.__block_size = block_size
		self.__initialized = False
	
	NAME_FORMAT = '{0}:{1}_{2}'
	
	@staticmethod
	def from_numpy(mat, name, block_size, redwrap):
		if len(mat.shape) != 2:
			raise BaseException('Shape of input matrix must be of size 2')
			
		rows = mat.shape[0]
		cols = mat.shape[1]
		m = Matrix(rows, cols, name, block_size, redwrap)
		
		for j in range(0, m.row_blocks()):
			for i in range(0, m.col_blocks()):
				block_name = m.block_name(j,i)
				redwrap.create_block(block_name, mat[max(j*block_size,0):min((j+1)*block_size,cols), max(i*block_size,0):min((i+1)*block_size,rows)])
		redwrap.redis.hmset(name + ':info', { 'block_size': block_size })
				
		m.__initialized = True	
		return m
	
	def delete(self):
		for block in self.block_names():
			self.__redwrap.redis.delete(block)
		self.__redwrap.redis.delete(self.__name + ':info')
		self.__initialized = False
	
	def from_name(name, block_size, redwrap):
		pass
	
	def block_size(self):
		return self.__block_size
	
	def dimension(self):
		return (self.__rows, self.__cols)
	
	def can_multiply_with(self, m):
		return self.__rows == m.__cols and self.__cols == m.__rows and self.block_size() == m.block_size()
	
	def name(self):
		return self.__name
	
	def row_blocks(self):
		return int(math.ceil(float(self.__rows) / self.__block_size))
		
	def col_blocks(self):
		return int(math.ceil(float(self.__cols) / self.__block_size))
	
	def block_name(self, row, col):
		if row < 0 or row > (self.__rows / self.__block_size):
			raise BaseException('Row does not exist')
		if col < 0 or col > (self.__cols / self.__block_size):
			raise BaseException('Col does not exist')
		
		return Matrix.NAME_FORMAT.format(self.__name, row, col)
	
	def row_block_names(self, row):
		if row < 0 or row > (self.__rows / self.__block_size):
			raise BaseException('Row does not exist')
		result = []
		y = self.row_blocks()
		
		for j in range(0,y):
			result.append(Matrix.NAME_FORMAT.format(self.__name, row, j))
		return result
		
	def col_block_names(self, col):
		if col < 0 or col > (self.__cols / self.__block_size):
			raise BaseException('Col does not exist')
		result = []
		x = self.col_blocks()
		
		for i in range(0,x):
			result.append(Matrix.NAME_FORMAT.format(self.__name, i, col))
		return result
		
	def block_names(self):
		x = self.col_blocks()
		y = self.row_blocks()

		result = []
		for j in range(0,y):
			for i in range(0,x):
				result.append(Matrix.NAME_FORMAT.format(self.__name, j, i))
		return result
	
	def print_blocks(self):
		for row in range(0,self.row_blocks()):
			for col in range(0,self.col_blocks()):
				n = self.__redwrap.get_block(self.block_name(row, col))
				print self.block_name(row, col)
				print str(n)
				print '----'
	
	def get_cell_value(self, row, col):
		block_row = int(math.floor(row / self.__block_size))
		block_col = int(math.floor(col / self.__block_size))
		offset_row = row % self.__block_size
		offset_col = col % self.__block_size
		block = self.__redwrap.get_block(self.block_name(block_row, block_col))
		return block[offset_row, offset_col]
	
	def get_numpy_block(self, row, col):
		return self.__redwrap.get_block(self.block_name(row, col))
		
	def get_numpy_matrix(self):
		#TODO: Take care of matrices with 1 block only
		m = None
		for row in range(0,self.row_blocks()):
			b = self.__redwrap.get_block(self.block_name(row, 0))
			#print self.block_name(row, 0)
			for col in range(1,self.col_blocks()):
				if row == 0 and col == 0:
					continue
				#print self.block_name(row, col)
				#print '---'
				n = self.__redwrap.get_block(self.block_name(row, col))
				b = numpy.concatenate((b, n), axis=1)
			if m is None:
				m = b 
			else:
				m = numpy.concatenate((m, b))
		return m
	
	def multiply(self, m, result_name):
		'''
			Multiplies the matrix with another one
		'''
		if not self.can_multiply_with(m):
			raise Exception('Dimensions do not match for multiplication')
		
		mult_job = jobs.Job(self.__redwrap)
		add_job = jobs.Job(self.__redwrap)
		
		mult_name = 'mmult(' + self.name() + ',' + m.name() + '):{0}_{1}_{2}'
		
		for col in range(0, self.col_blocks()):
			for row in range(0,self.row_blocks()):
				for col2 in range(0,self.row_blocks()):
					mult_cmd = command_builder.build_command('MMULT', 	self.block_name(row, col),
																		m.block_name(col, col2),
																		mult_name.format(col, row, col2))
					mult_job.add_subjob(mult_cmd)
		
		if not mult_job.execute():
			raise exceptions.MatrixOperationException('Could not multiply blocks')
		
		for col in range(0, m.col_blocks()):
			for row in range(0,self.row_blocks()):
				cb = command_builder.CommandBuilder('MADD')
				for col2 in range(0,self.row_blocks()):
					cb.add_param(mult_name.format(col2, row, col))
				
				cb.add_param(Matrix.NAME_FORMAT.format(result_name, row, col))
				add_job.add_subjob(cb.getCmdString())
				
		if not add_job.execute():
			raise exceptions.MatrixOperationException('Could not add up matrix parts')
			
		res = Matrix(self.__rows, m.__cols, result_name, self.__block_size, self.__redwrap)
		res.__initialized = True
		return res
		
	def transpose(self, result_name):
		'''
			Switches rows and columns of the matrix
			[[1,2,3],			[[1,4,7],
			 [4,5,6],	becomes	 [2,5,8],
			 [7,8,9]]			 [3,6,9]]
		'''
		trans_job = jobs.Job(self.__redwrap)
		for col in range(0, self.col_blocks()):
			for row in range(0,self.row_blocks()):
				transp_cmd = command_builder.build_command('MTRANS', self.block_name(row, col), Matrix.NAME_FORMAT.format(result_name, col, row))
				trans_job.add_subjob(transp_cmd)
		if not trans_job.execute():
			raise exceptions.MatrixOperationException('Transposing the matrix failed')
			
		res = Matrix(self.__cols, self.__rows, result_name, self.__block_size, self.__redwrap)
		res.__initialized = True
		return res