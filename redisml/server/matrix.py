from jobs import jobs
from jobs import matrix_jobs
from jobs import kmeans_jobs
from redis import Redis
import redisml.shared.redis_constants as const
import redisml.shared.commands as cmd
from redisml.shared.redis_wrapper import RedisWrapper
import exceptions
import math
import numpy
import json
import random
import sys
import types

class MatrixFactory:
    mcounter = 0
    def __init__(self, context):
        self.context = context
    
    @staticmethod
    def getRandomMatrixName():
        MatrixFactory.mcounter += 1
        rnd = random.randint(0,99999)
        return const.MATRIX_NAME_FORMAT.format(str(MatrixFactory.mcounter), str(rnd))
        
    def matrix_from_numpy(self, mat, name=None):
        return Matrix.from_numpy(mat, self.context, name=name)
    
    def matrix_from_name(self, name):
        return Matrix.from_name(name, self.context)
        
    def random_matrix(self, rows, cols, name=None):
        return Matrix.random(rows, cols, self.context, name=name)
        
    def matrix_from_scalar(self, scalar, rows, cols, name=None):
        return Matrix.from_scalar(scalar, rows, cols, self.context, name=name)
    
class Matrix:
    def __init__(self, rows, cols, name, context):
        self.__rows = rows
        self.__cols = cols
        self.shape = (self.__rows, self.__cols)
        self.__name = name
        self.__persist = False
        self.context = context
        self.__block_size = context.block_size
        self.context.redis_master.hmset(const.INFO_FORMAT.format(name), { 'block_size': context.block_size, 'rows' : rows, 'cols' : cols })
              
    def set_persistence(self, persist):
        """
            Sets whether this matrix should be deleted on the redis server once the object is deleted
        """
        self.__persist = persist

    #
    # Static methods
    #
    
    @staticmethod
    def from_numpy(mat, context, name=None):
        """
            Creates a matrix from a numpy matrix
        """
        if len(mat.shape) != 2:
            raise BaseException('Shape of input matrix must be of size 2')
        if name == None:
            name = MatrixFactory.getRandomMatrixName()
            
        # Check if matrix already exists
        if context.redis_master.exists(const.INFO_FORMAT.format(name)):
            raise BaseException('A matrix with this name already exists on the redis server')
        
        rows = mat.shape[0]
        cols = mat.shape[1]

        redwrap = RedisWrapper(context.redis_master, context.key_manager)

        m = Matrix(rows, cols, name, context)
        # Separate blocks and send them to the redis server
        for j in range(0, m.row_blocks()):
            for i in range(0, m.col_blocks()):
                block_name = m.block_name(j,i)
                block = mat[max(j*context.block_size,0):min((j+1)*context.block_size,rows+1),
                            max(i*context.block_size,0):min((i+1)*context.block_size,cols+1)]
                
                redwrap.create_block(block_name, block)
        return m
    
    @staticmethod
    def from_name(name, context):
        """
            Creates a matrix object from a name.
            This is used when the matrix blocks are already stored on the redis server, e.g. because the matrix was persisted earlier
        """
        info = redis.hgetall(const.INFO_FORMAT.format(name))
        if info:
            if info['block_size'] != context.block_size:
                raise BaseException('Matrix has the wrong block size')
            return Matrix(info['rows'], info['cols'], name, context)
        else:
            raise BaseException('Matrix with the name ' + name + ' does not exist on database')
     
    @staticmethod       
    def from_scalar(scalar, rows, cols, context, name=None):
        """
            Creates a matrix where each element is the given scalar value
        """
        if name == None:
            name = MatrixFactory.getRandomMatrixName()
        create_job = matrix_jobs.CreateMatrixJob(context, scalar, rows, cols, name)
        create_job.run()
        return Matrix(rows, cols, name, context)
    
    @staticmethod  
    def random(rows, cols, context, name=None):
        """
            Creates a matrix with uniformly distributed random content between 0.0 and 1.0
        """
        if name == None:
            name = MatrixFactory.getRandomMatrixName()
        create_job = matrix_jobs.CreateMatrixJob(context, 'rand', rows, cols, name)
        create_job.run()
        return Matrix(rows, cols, name, context)
    
    #
    # Container emulation
    #
    def __len__(self):
        return self.__rows * self.__cols
    
    def __getitem__(self, i):
        if isinstance(i, slice):
            indices = i.indices(3)
            if indices[2] != 1:
                raise Exception('Step sizes other than 1 are not allowed')
            return self.slice(indices[0], indices[1]+1, 0, self.__cols)
        elif isinstance(i, tuple):
            idx1 = i[0].indices(3)
            idx2 = i[1].indices(3)
            return self.slice(idx1[0], idx1[1] - idx1[0], idx2[0], idx2[1] - idx2[0])
    
    #
    # Getters for matrix properties
    #
    def block_size(self):
        """
            Returns this matrix' block size
        """
        return self.__block_size
    
    def can_multiply_with(self, m):
        """
            Returns a boolean indicating whether the dot product of this matrix with the given one can be calculated
        """
        return self.__cols == m.__rows and self.block_size() == m.block_size()

    def name(self):
        """
            Returns this matrix' name
        """
        return self.__name
    
    def row_blocks(self):
        """
            Returns the number of row blocks this matrix is divided into
        """
        return int(math.ceil(float(self.__rows) / self.__block_size))
        
    def col_blocks(self):
        """
            Returns the number of column blocks this matrix is divided into
        """
        return int(math.ceil(float(self.__cols) / self.__block_size))
    
    def block_name(self, row, col):
        """
            Returns the redis key for the block at the given index
        """
        if row < 0 or row > (self.__rows / self.__block_size):
            raise BaseException('Row does not exist')
        if col < 0 or col > (self.__cols / self.__block_size):
            raise BaseException('Col does not exist')
        
        return self.context.key_manager.get_block_name(self.__name, row, col)
    
    def row_block_names(self, row):
        """
            Returns a list of keys of all blocks in a given row
        """
        result = []
        y = self.row_blocks()
        
        for j in range(0,y):
            result.append(self.block_name(row, j))
        return result
        
    def col_block_names(self, col):
        """
            Returns a list of keys of all blocks in a given column
        """
        result = []
        x = self.col_blocks()
        
        for i in range(0,x):
            result.append(self.block_name(i, col))
        return result
        
    def block_names(self):
        """
            Returns a list with all block keys
        """
        x = self.col_blocks()
        y = self.row_blocks()

        result = []
        for j in range(0,y):
            for i in range(0,x):
                result.append(self.block_name(j, i))
        return result
    
    def is_quadratic(self):
        """
           Returns true if the number of rows equals the number of columns 
        """
        return self.__rows == self.__cols
    
    def print_blocks(self):
        """
            Prints each block
        """
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        for row in range(0, self.row_blocks()):
            for col in range(0, self.col_blocks()):
                n = redwrap.get_block(self.block_name(row, col))
                print self.block_name(row, col)
                print str(n)
                print '----'
    
    def set_cell_value(self, row, col, val):
        """
            Sets the value of a single matrix cell
        """
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        block_row = int(math.floor(row / self.__block_size))
        block_col = int(math.floor(col / self.__block_size))
        offset_row = row % self.__block_size
        offset_col = col % self.__block_size
        block_name = self.block_name(block_row, block_col)
        block = redwrap.get_block(block_name)
        block[offset_row, offset_col] = val
        redwrap.create_block(block_name, block)
    
    def get_cell_value(self, row, col):
        """
            Returns the value of a single matrix cell
        """
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        block_row = int(math.floor(row / self.__block_size))
        block_col = int(math.floor(col / self.__block_size))
        offset_row = row % self.__block_size
        offset_col = col % self.__block_size
        block = redwrap.get_block(self.block_name(block_row, block_col))
        return block[offset_row, offset_col]
    
    def get_numpy_block(self, row, col):
        """
            Returns a block as numpy matrix
        """
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        return redwrap.get_block(self.block_name(row, col))
        
    def get_numpy_matrix(self):
        """
            Concatenates all blocks of this matrix and returns one big numpy matrix
        """
        m = None
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        for row in range(0,self.row_blocks()):
            b = redwrap.get_block(self.block_name(row, 0))
            #print self.block_name(row, 0)
            for col in range(1,self.col_blocks()):
                if row == 0 and col == 0:
                    continue
                #print self.block_name(row, col)
                #print '---'
                n = redwrap.get_block(self.block_name(row, col))
                b = numpy.concatenate((b, n), axis=1)
            if m is None:
                m = b 
            else:
                m = numpy.concatenate((m, b))
        return m
    
    #
    # Matrix operations
    #
    def slice(self, row, num_rows, col, num_cols, result_name=None):
        
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        # Check if given values are valid
        if row + num_rows > self.__rows or (num_rows < 0 and row + num_rows < 0):
            raise Exception('Row index out of bounds')
        if col + num_cols > self.__cols or (num_cols < 0 and col + num_cols < 0):
            raise Exception('Column index out of bounds')
        # Handle negative indices
        if row < 0:
            row = self.__rows + row
        if num_rows < 0:
            row = row + num_rows
            num_rows = -num_rows
            
        if col < 0:
            col = self.__cols + col
        if num_cols < 0:
            col = col + num_cols
            num_cols = -num_cols
        
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        row_blocks = num_rows / self.__block_size
        if num_rows % self.__block_size != 0:
            row_blocks += 1
        col_blocks = num_cols / self.__block_size
        if num_cols % self.__block_size != 0:
            col_blocks += 1                       
        # Iterate the blocks of the new slice
        for r in range(0, row_blocks):
            for c in range(0, col_blocks):
                start_row = row + r * self.__block_size
                end_row = min(row + num_rows, start_row + self.__block_size)
                start_col = col + c * self.__block_size
                end_col = min(col + num_cols, start_col + self.__block_size)
                
                # Iterate the blocks of the current matrix that intersect with the current block of the new slice
                # and patch them together
                a = None
                for i in range(start_row / self.__block_size, (end_row-1) / self.__block_size + 1):
                    row_b = None
                    for j in range(start_col / self.__block_size, (end_col-1) / self.__block_size + 1):
                        n = redwrap.get_block(self.block_name(i, j))
                        if row_b == None:
                            row_b = n
                        else:
                            row_b = numpy.concatenate((row_b, n), axis=1)
                    if a == None:
                        a = row_b
                    else:
                        a = numpy.concatenate((a, row_b), axis=0)

                # Now we have a matrix so big that the whole current block of the new slice as defined by r and c fits in it
                # Use numpy slicing to get the block from this matrix
                mr = min(row + self.__block_size, a.shape[0])
                mc = min(col + self.__block_size, a.shape[1])
                sc = col % self.__block_size
                sr = row % self.__block_size
                if r == row_blocks-1 and (row + num_rows) % self.__block_size != 0:
                    mr = min(mr, (row + num_rows) % self.__block_size)   
                if c == col_blocks-1 and (col + num_cols) % self.__block_size != 0:
                    mc = min(mc, (col + num_cols) % self.__block_size)
                if mr == row and row % self.__block_size != 0:
                    mr += 1
                if mc == col and col % self.__block_size != 0:
                    mc += 1
                
                block = a[sr:mr,sc:mc]
                redwrap.create_block(self.context.key_manager.get_block_name(result_name, r, c), block)

        return Matrix(num_rows, num_cols, result_name, self.context)
        
    def scalar_divide(self, scalar, result_name=None):
        """
            Divides the matrix by a scalar
        """
        return self.__ms(scalar, '/', result_name)
        
    def scalar_rdivide(self, scalar, result_name=None):
        """
            Calculates a matrix where a scalar is divided by each element of this matrix 
        """
        return self.__ms(scalar, 'rdiv', result_name)
        
    def scalar_multiply(self, scalar, result_name=None):
        """
            Multiplies this matrix with a scalar
        """
        return self.__ms(scalar, '*', result_name)
        
    def scalar_add(self, scalar, result_name=None):
        """
            Adds a scalar to each cell of the matrix
        """
        return self.__ms(scalar, '+', result_name)
        
    def scalar_subtract(self, scalar, result_name=None):
        """
            Subtracts a scalar from each cell of this matrix
        """
        return self.__ms(scalar, '-', result_name)
        
    def __ms(self, scalar, op, result_name=None):
        """
            Multiplies the matrix with a scalar
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        ms_job = matrix_jobs.MatrixScalarJob(self.context, self, scalar, op, result_name)
        ms_job.run()
        
        res = Matrix(self.__rows, self.__cols, result_name, self.context)
        return res
    
    def dot(self, m, result_name=None, transpose_self=False, transpose_m=False, negate_self=False, negate_m=False):
        """
            Multiplies the matrix with another one
        """
        self.__check_blocksize(m)
        if not self.can_multiply_with(m):
            raise Exception('Dimensions do not match for multiplication: ' + str(self.shape) + ' - ' + str(m.shape))
        
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        # Multiply blocks
        mult_name = 'mmult(' + self.name() + ',' + m.name() + '):{0}:{1}_{2}_{3}'
        mult_job = matrix_jobs.MultiplicationJob(self.context, self, m,
                                                    transpose_self,
                                                    transpose_m,
                                                    negate_self,
                                                    negate_m,
                                                    mult_name)
        mult_job.run()
        # Merge parts generated by the first job
        add_job = matrix_jobs.MultiplicationMergeJob(self.context, self.row_blocks(), m.col_blocks(), mult_name, result_name)
        add_job.run()
        
        res = Matrix(self.__rows, m.__cols, result_name, self.context)
        return res
    
    def cw_add(self, m, result_name=None):
        """
            Cellwise addition
        """
        return self.__cw(m, "+", result_name)
    
    def cw_subtract(self, m, result_name=None):
        """
            Cellwise subtraction
        """
        return self.__cw(m, "-", result_name)
        
    def cw_multiply(self, m, result_name=None):
        """
            Cellwise multiplication
        """
        return self.__cw(m, "*", result_name)
        
    def cw_divide(self, m, result_name=None):
        """
            Cellwise division
        """
        return self.__cw(m, "/", result_name)
    
    def __cw(self, m, op, result_name=None): 
        """
           Helper method for different cellwise functions 
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        self.__check_blocksize(m)
        cw_job = matrix_jobs.BinaryExpressionJob(self.context, self, m, 'x{0}y'.format(op), result_name)
        cw_job.run()
        res = Matrix(self.__rows, self.__cols, result_name, self.context)
        return res
        
    def transpose(self, result_name=None):
        """
            Switches rows and columns of the matrix
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        trans_job = matrix_jobs.TransposeJob(self.context, self, result_name)
        trans_job.run()
        res = Matrix(self.__cols, self.__rows, result_name, self.context)
        return res
    
    def __inner_aggr(self, aggr_op, expr, axis):
        """
            Helper method for different aggregation functions
        """
        aggr_job = jobs.Job(self.context)
        prefix = 'aggr_' + aggr_op + '(' + self.__name + ',' + str(axis) + ')'

        # First sum up each block
        for col in range(0, self.col_blocks()):
            for row in range(0,self.row_blocks()):
                mname = self.context.key_manager.get_block_name(prefix, col, row)
                aggr_cmd = cmd.build_command(cmd.AGGREGATEOP, self.block_name(row, col), cmd.escape_expression(expr), axis, aggr_op, mname)
                aggr_job.add_subjob(aggr_cmd)
        try:
            aggr_job.execute()
        except exceptions.JobException as e:
            raise exceptions.MatrixOperationException(str(e), cmd.AGGREGATEOP)
        
        return prefix

    def __aggr(self, inner_aggr, outer_aggr, axis=None, expr='x', result_name=None):
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
            
        prefix = self.__inner_aggr(inner_aggr, expr, axis)
        aggr_job = jobs.Job(self.context)
        
        if axis == 0:
            for col in range(0, self.col_blocks()):
                bin_cb = cmd.CommandBuilder(cmd.BINARYMATRIXOP)
                del_cb = cmd.CommandBuilder(cmd.DELETE)
                for row in range(0, self.row_blocks()):
                    mname = self.context.key_manager.get_block_name(prefix, col, row)
                    bin_cb.add_param(mname)
                    del_cb.add_param(mname)
                bin_cb.add_param(outer_aggr)
                bin_cb.add_param(self.context.key_manager.get_block_name(result_name, 0, col))
                aggr_job.add_subjob(bin_cb.join(del_cb))
            aggr_job.execute()
            return Matrix(1, self.__cols, result_name, self.context)
        elif axis == 1:
            for row in range(0, self.row_blocks()):
                bin_cb = cmd.CommandBuilder(cmd.BINARYMATRIXOP)
                del_cb = cmd.CommandBuilder(cmd.DELETE)
                for col in range(0,self.col_blocks()):
                    mname = self.context.key_manager.get_block_name(prefix, col, row)
                    bin_cb.add_param(mname)
                    del_cb.add_param(mname)
                bin_cb.add_param(outer_aggr)
                bin_cb.add_param(self.context.key_manager.get_block_name(result_name, row, 0))
                aggr_job.add_subjob(bin_cb.join(del_cb))
            aggr_job.execute()
            return Matrix(self.__rows, 1, result_name, self.context)
        elif axis == None:
            total = None
            redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
            for col in range(0, self.col_blocks()):
                for row in range(0,self.row_blocks()):
                    key = self.context.key_manager.get_block_name(prefix, col, row)
                    val = float(redwrap.get_value(key))
                    if total == None:
                        total = val
                    else:
                        total = eval(outer_aggr, { 'numpy' : numpy, 'x' : total, 'y' : val })
                    redwrap.delete_block(key)
            return total
    
    def sum(self, expr='x', axis=None):
        return self.__aggr('sum', 'x+y', axis=axis, expr=expr)
    
    def min(self, expr='x', axis=None):
        return self.__aggr('min', 'numpy.minimum(x,y)', axis=axis, expr=expr)
    
    def max(self, expr='x', axis=None):
        return self.__aggr('max', 'numpy.maximum(x,y)', axis=axis, expr=expr)
    
    def trace(self):
        """
            Computes the trace of the matrix if it is square (The sum of all diagonal elements)
        """
        
        if not self.is_quadratic():
            raise exceptions.MatrixOperationException('Can only compute trace of a square matrix')

        output_key = 'mtrace(' + self.name() + ')'
        trace_job = matrix_jobs.TraceJob(self.context, self, output_key)
        trace_job.run()
        
        results = self.context.redis_master.lrange(output_key, 0, -1)
        sum = 0
        for r in results:
            sum += float(r)
        self.context.redis_master.delete(output_key)
        return sum
        
    def k_means_distance(self, centers, result_name=None):
        """
            Computes the distance between each row and each of the given center vectors for k-means
        """
        if centers.shape[1] != self.__cols:
            raise BaseException('Dimensions of matrix and centers do not match')
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        prefix = 'dist(' + self.__name + ',' + centers.name() + ')';
        dist_job = kmeans_jobs.KMeansDistanceJob(self.context, self, centers, prefix)
        
        parts = dist_job.run()
        
        for p in range(0,len(parts)):
            part_name = parts[p]
            m = self.context.redis_master.lpop(part_name)
            sum = None
            while m != None:
                if sum == None:
                    sum = numpy.loads(m)
                else:
                    sum += numpy.loads(m)
                m = self.context.redis_master.lpop(part_name)
                
            self.context.redis_master.delete(part_name)
            redwrap.create_block(self.context.key_manager.get_block_name(result_name, p, 0), numpy.sqrt(sum))
        
        res = Matrix(self.__rows, centers.shape[0], result_name, self.context)
        return res
    
    def k_means_recalc(self, dist, result_name=None):
        """
            Calculates new k-means centers from a previously computed distance matrix
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        num_centers = dist.shape[1]
        prefix = 'center(' + self.__name + ',' + dist.name() + ')'
        cnt_prefix = 'counter(' + self.__name + ',' + dist.name() + ')_'
        
        recalc_job = kmeans_jobs.KMeansRecalculationJob(self.context, self, dist, prefix, cnt_prefix)
        recalc_job.run()
        
        for col in range(0, self.col_blocks()):
            conc = None
            for center in range(0, num_centers):
                name = prefix + '_' + str(col) + '_' + str(center)
                m = self.context.redis_master.lpop(name)
                sum = None
                while m != None:
                    if sum == None:
                        sum = numpy.loads(m)
                    else:
                        sum += numpy.loads(m)
                    m = self.context.redis_master.lpop(name)
                self.context.redis_master.delete(name)
                # Sum is only a row. To make it a matrix that we can concatenate, we have to wrap it
                if len(sum.shape) == 1:
                    sum = numpy.matrix([sum])
                num_records = self.context.redis_master.get(cnt_prefix + str(center))
                num_records = float(num_records) if num_records != None else 1 #TODO: This is an error
                if conc == None:
                    conc = sum / num_records
                else:
                    conc = numpy.concatenate((conc, sum / num_records), axis=0)

            redwrap.create_block(self.context.key_manager.get_block_name(result_name, 0, col), conc)
        res = Matrix(num_centers, self.__cols, result_name, self.context)
        return res
    
    def equals(self, m):
        """
            Checks if two matrices contain the same values
        """
        if self.shape != m.shape:
            return False

        result_key = 'equal(' + self.name() + ',' + m.name() + ')'
        equals_job = matrix_jobs.EqualJob(self.context, self, m, result_key)
        equals_job.run()
        
        pipe = self.context.redis_master.pipeline()
        for i in range(self.row_blocks() * self.col_blocks()):
            pipe.lpop(result_key)
        for val in pipe.execute():
            if int(val) == 0:
                self.context.redis_master.delete(result_key)
                return False
        return True
    
    def negate(self):
        """
            Negates every element in the matrix
        """
        return self * -1
    
    def count(self):
        """
            For each occurring value per column this method counts the number of occurrences
        """
        prefix = 'count(' + self.name() + ')_'
        count_job = matrix_jobs.CountJob(self.context, self, prefix)
        count_job.run()
        
        output = []
        for col in range(0, self.col_blocks()):
            max = self.__block_size
            if col == self.col_blocks()-1 and self.__cols % self.__block_size > 0:
                max = self.__cols % self.__block_size
            
            for i in range(0, max):
                dict = {}
                key = prefix + str(col) + ':' + str(i)
                values = self.context.redis_master.smembers(key)
                for v in values:
                    k = key + ':' + v
                    count = int(self.context.redis_master.get(k))
                    dict[v] = count
                    self.context.redis_master.delete(k)
                output.append(dict)
                self.context.redis_master.delete(key)
        return output
    
    def probabilities(self):
        """
            For each occurring value per column this method calculates the probability that this value actually occurs in a certain row
        """
        c = self.count()
        for col in c:
            for key, val in col.items():
                col[key] = float(val) / self.__rows
        return c
        
    def normalize(self, result_name):
        """
            Divides each element in the matrix by the highest element
        """
        return self.scalar_divide(self.max(), result_name=result_name)
        
    def vector_norm(self, ord):
        """
            Calculates the vector norm of the given order
        """
        expr = 'numpy.power(x, ' + str(ord) + ')'
        sum = self.sum(expr=expr)
        return sum**(1.0/ord)
        
    def toScalar(self):
        """
            Returns a scalar if the matrix consists of only one cell
        """
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        if self.shape[0] != 1 or self.shape[1] != 1:
            raise exceptions.MatrixOperationException('Cannot convert a matrix with more than one column and row to a scalar', 'MATRIX2SCALAR')
        return redwrap.get_block(self.block_name(0,0))[0,0]
    
    def __check_blocksize(self, m):
        """
            Checks if a matrix has the same block size as this one.
            If not, an exception is thrown.
        """
        if self.block_size() != m.block_size():
            raise Exception("Block sizes do not match")
    
    def __build_modifier_string(self, bools, modifiers):
        """
            Builds a modifier string for matrix multiplication
        """
        if len(bools) != len(modifiers):
            raise Exception("Lengths of bools and modifiers must match")
        for m in modifiers:
            if m == "n" or m == ";":
                raise Exception("Modifiers 'n' and ';' are reserved")
        result = ""
        for i in range(0, len(bools)):
            if bools[i]:
                result += modifiers[i]
        if len(result) == 0:
            result = "n"
        return result
        
    def __delete(self):
        redwrap = RedisWrapper(self.context.redis_master, self.context.key_manager)
        for block in self.block_names():
            redwrap.delete_block(block)
        self.context.redis_master.delete(const.INFO_FORMAT.format(self.__name))
    
    def __del__(self):
        if not self.__persist:
            try:
                self.__delete()
            finally:
                pass

    
    def __str__(self):
        return str(self.get_numpy_matrix())
        
    def __repr__(self):
        return self.__str__()
        
    # ======= Operators =======
    
    NUMBER_TYPES = (types.IntType, types.LongType, types.FloatType, types.ComplexType)
    
    def __neg__(self):
        return self.negate()
    
    def __add__(self, other):
        if isinstance(other, Matrix):
            return self.cw_add(other)
        elif isinstance(other, Matrix.NUMBER_TYPES):
            return self.scalar_add(other)
            
    def __sub__(self, other):
        if isinstance(other, Matrix):
            return self.cw_subtract(other)
        elif isinstance(other, Matrix.NUMBER_TYPES):
            return self.scalar_subtract(other)
            
    def __mul__(self, other):
        if isinstance(other, Matrix):
            return self.cw_multiply(other)
        elif isinstance(other, Matrix.NUMBER_TYPES):
            return self.scalar_multiply(other)
            
    def __div__(self, other):
        if isinstance(other, Matrix):
            return self.cw_divide(other)
        elif isinstance(other, Matrix.NUMBER_TYPES):
            return self.scalar_divide(other)
            
    def __truediv__(self, other):
        return self.__div__(other)
        
    def __radd__(self, other):
        return self.__add__(other)
            
    def __rsub__(self, other):
        return self.negate().__add__(other)
            
    def __rmul__(self, other):
        return self.__mul__(other)
        
    def __rdiv__(self, other):
        return self.scalar_rdivide(other)
        
    def __rtruediv__(self, other):
        return self.__rdiv__(other)