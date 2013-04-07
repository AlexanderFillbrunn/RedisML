from jobs import jobs
from jobs import matrix_jobs
from jobs import kmeans_jobs
from redis import Redis
import redisml.shared.redis_constants as const
import redisml.server.command_builder as command_builder
from redisml.shared.redis_wrapper import RedisWrapper
from redisml.shared import command_names as cmdnames
import exceptions
import math
import numpy
import json
import random
import sys

class MatrixFactory:
    mcounter = 0
    def __init__(self, redis, key_mngr, block_size):
        self.redis = redis
        self.block_size = block_size
        self.key_manager = key_mngr
    
    @staticmethod
    def getRandomMatrixName():
        MatrixFactory.mcounter += 1
        rnd = random.randint(0,99999)
        return const.MATRIX_NAME_FORMAT.format(str(MatrixFactory.mcounter), str(rnd))
        
    def matrix_from_numpy(self, mat, name=''):
        return Matrix.from_numpy(mat, self.block_size, self.redis, self.key_manager, name=name if not len(name) == 0 else MatrixFactory.getRandomMatrixName())
    
    def matrix_from_name(self, name):
        return Matrix.from_name(name, self.redis, self.key_manager)
    
class Matrix:
    def __init__(self, rows, cols, name, block_size, redis, key_mngr, initialized=False):
        self.__rows = rows
        self.__cols = cols
        self.__name = name
        self.__persist = False
        self.__key_mngr = key_mngr
        self.__redis = redis
        self.__block_size = block_size
        self.__initialized = initialized
        
        # Register on redis server
        if initialized:
            self.__redis.hmset(const.INFO_FORMAT.format(name), { 'block_size': block_size, 'rows' : rows, 'cols' : cols })
    
    def __del__(self):
        if self.__initialized:
            if not self.__persist:
                try:
                    self.delete()
                finally:
                    pass
                
    def set_persistence(self, persist):
        self.__persist = persist
    
    def delete(self):
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        for block in self.block_names():
            redwrap.delete_block(block)
        self.__redis.delete(const.INFO_FORMAT.format(self.__name))
        self.__initialized = False
    
    #
    # Static methods
    #
    @staticmethod
    def from_file(filename, block_size, name=None):
        pass
    
    @staticmethod
    def from_numpy(mat, block_size, redis, key_mngr, name=None):
        if len(mat.shape) != 2:
            raise BaseException('Shape of input matrix must be of size 2')
        if name == None:
            name = MatrixFactory.getRandomMatrixName()
            
        # Check if matrix already exists
        if redis.exists(const.INFO_FORMAT.format(name)):
            raise BaseException('A matrix with this name already exists on the redis server')
        
        rows = mat.shape[0]
        cols = mat.shape[1]


        redwrap = RedisWrapper(redis, key_mngr)

        m = Matrix(rows, cols, name, block_size, redis, key_mngr, True)
        # Separate blocks and send them to the redis server
        for j in range(0, m.row_blocks()):
            for i in range(0, m.col_blocks()):
                block_name = m.block_name(j,i)
                block = mat[max(j*block_size,0):min((j+1)*block_size,rows+1),
                            max(i*block_size,0):min((i+1)*block_size,cols+1)]
                
                redwrap.create_block(block_name, block)
        return m
    
    @staticmethod
    def from_name(name, redis, key_mngr):
        info = redis.hgetall(const.INFO_FORMAT.format(name))
        if info:
            return Matrix(info['rows'], info['cols'], name, info['block_size'], redis, key_mngr, True)
        else:
            raise BaseException('Matrix with the name ' + name + ' does not exist on database')
    #
    # Getters for matrix properties
    #
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
        """
            Returns the redis key for the block at the given index
        """
        if row < 0 or row > (self.__rows / self.__block_size):
            raise BaseException('Row does not exist')
        if col < 0 or col > (self.__cols / self.__block_size):
            raise BaseException('Col does not exist')
        
        return self.__key_mngr.get_block_name(self.__name, row, col)
    
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
        return self.__rows == self.__cols
    
    def print_blocks(self):
        """
            Prints each block
        """
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        for row in range(0, self.row_blocks()):
            for col in range(0, self.col_blocks()):
                n = redwrap.get_block(self.block_name(row, col))
                print self.block_name(row, col)
                print str(n)
                print '----'
    
    def get_cell_value(self, row, col):
        """
            Returns the value of a single matrix cell
        """
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
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
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        return redwrap.get_block(self.block_name(row, col))
        
    def get_numpy_matrix(self):
        """
            Concatenates all blocks of this matrix and returns one big numpy matrix
        """
        m = None
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
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
    def scalar_divide(self, scalar, result_name=None):
        return self.__ms(scalar, "/", result_name)
        
    def scalar_multiply(self, scalar, result_name=None):
        return self.__ms(scalar, "*", result_name)
        
    def scalar_add(self, scalar, result_name=None):
        return self.__ms(scalar, "+", result_name)
        
    def scalar_subtract(self, scalar, result_name=None):
        return self.__ms(scalar, "-", result_name)
        
    def __ms(self, scalar, op, result_name=None):
        """
            Multiplies the matrix with a scalar
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        ms_job = matrix_jobs.MatrixScalarJob(self.__redis, self.__key_mngr, self, scalar, op, result_name)
        ms_job.run()
        
        res = Matrix(self.__rows, self.__cols, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def multiply(self, m, result_name=None, transpose_self=False, transpose_m=False, negate_self=False, negate_m=False):
        """
            Multiplies the matrix with another one
        """
        self.__check_blocksize(m)
        if not self.can_multiply_with(m):
            raise Exception('Dimensions do not match for multiplication')
        
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        # Multiply blocks
        mult_name = 'mmult(' + self.name() + ',' + m.name() + '):{0}:{1}_{2}_{3}'
        mult_job = matrix_jobs.MultiplicationJob(self.__redis, self.__key_mngr, self, m,
                                                    transpose_self,
                                                    transpose_m,
                                                    negate_self,
                                                    negate_m,
                                                    mult_name)
        mult_job.run()
        # Merge parts generated by the first job
        add_job = matrix_jobs.MultiplicationMergeJob(self.__redis, self.__key_mngr, self.row_blocks(), m.col_blocks(), mult_name, result_name)
        add_job.run()
        
        res = Matrix(self.__rows, m.__cols, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def cw_add(self, m, result_name=None):
        return self.__cw(m, "+", result_name)
    
    def cw_subtract(self, m, result_name=None):
        return self.__cw(m, "-", result_name)
        
    def cw_multiply(self, m, result_name=None):
        return self.__cw(m, "*", result_name)
        
    def cw_divide(self, m, result_name=None):
        return self.__cw(m, "/", result_name)
    
    def __cw(self, m, op, result_name=None):
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        self.__check_blocksize(m)
        
        cw_job = matrix_jobs.CellwiseOperationJob(self.__redis, self.__key_mngr, self, m, op, result_name)
        cw_job.run()
        res = Matrix(self.__rows, self.__cols, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
        
    def transpose(self, result_name=None):
        """
            Switches rows and columns of the matrix
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        trans_job = matrix_jobs.TransposeJob(self.__redis, self.__key_mngr, self, result_name)
        trans_job.run()
        res = Matrix(self.__cols, self.__rows, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def col_sums(self, result_name=None, expr='x'):
        """
            Sums up each col and returns a vector of all sums
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        colsum_job = jobs.Job(self.__redis, self.__key_mngr)
        add_job = jobs.Job(self.__redis, self.__key_mngr)
        prefix = 'colsum(' + self.__name + ')'
        
        # First sum up each block
        for col in range(0, self.col_blocks()):
            for row in range(0,self.row_blocks()):
                mname = self.__key_mngr.get_block_name(prefix, col, row)
                colsum_cmd = command_builder.build_command('COLSUM', self.block_name(row, col), expr, mname)
                colsum_job.add_subjob(colsum_cmd)
        try:
            colsum_job.execute()
        except exceptions.JobException as e:
            raise exceptions.MatrixOperationException(str(e), 'COLSUM')
        
        # Now sum up the vectors for each row
        for col in range(0, self.col_blocks()):
            add_cb = command_builder.CommandBuilder('MADD')
            del_cb = command_builder.CommandBuilder('DEL')
            for row in range(0,self.row_blocks()):
                mname = self.__key_mngr.get_block_name(prefix, col, row)
                add_cb.add_param(mname)
                del_cb.add_param(mname)
            add_cb.add_param(self.__key_mngr.get_block_name(result_name, 0, col))
            add_job.add_subjob(add_cb.join(del_cb))
        try:
            add_job.execute()
        except exceptions.JobException as e:    
            raise exceptions.MatrixOperationException(str(e), 'MADD')
        
        res = Matrix(1, self.__cols, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
        
    def col_avg(self, result_name=None, expr='x'):
        m = self.col_sums(expr=expr)
        return m.scalar_divide(self.__rows, result_name=result_name)
        
    def row_sums(self, result_name=None, expr='x'):
        """
            Sums up each row and returns a vector of all sums
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        rowsum_job = jobs.Job(self.__redis, self.__key_mngr)
        add_job = jobs.Job(self.__redis, self.__key_mngr)
        prefix = 'rowsum(' + self.__name + ')'
        
        # First sum up each block
        for col in range(0, self.col_blocks()):
            for row in range(0,self.row_blocks()):
                mname = self.__key_mngr.get_block_name(prefix, col, row)
                rowsum_cmd = command_builder.build_command('ROWSUM', self.block_name(row, col), expr, mname)
                rowsum_job.add_subjob(rowsum_cmd)
        try:
            rowsum_job.execute()
        except exceptions.JobException as e:
            raise exceptions.MatrixOperationException(str(e), 'ROWSUM')
        
        # Now sum up the vectors for each column
        for row in range(0, self.row_blocks()):
            add_cb = command_builder.CommandBuilder('MADD')
            del_cb = command_builder.CommandBuilder('DEL')
            for col in range(0,self.col_blocks()):
                mname = self.__key_mngr.get_block_name(prefix, col, row)
                add_cb.add_param(mname)
                del_cb.add_param(mname)
            add_cb.add_param(self.__key_mngr.get_block_name(result_name, row, 0))
            add_job.add_subjob(add_cb.join(del_cb))
        try:
            add_job.execute()
        except exceptions.JobException as e:
            raise exceptions.MatrixOperationException(str(e), 'MADD')
        
        res = Matrix(self.__rows, 1, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def trace(self):
        """
            Computes the trace of the matrix if it is square (The sum of all diagonal elements)
        """
        
        if not self.is_quadratic():
            raise exceptions.MatrixOperationException('Can only compute trace of a square matrix')

        output_key = 'mtrace(' + self.name() + ')'
        trace_job = matrix_jobs.TraceJob(self.__redis, self.__key_mngr, self, output_key)
        trace_job.run()
        
        results = self.__redis.lrange(output_key, 0, -1)
        sum = 0
        for r in results:
            sum += float(r)
        self.__redis.delete(output_key)
        return sum
        
    def k_means_distance(self, centers, result_name=None):
        """
            Computes the distance between each row and each of the given center vectors for k-means
        """
        if centers.dimension()[1] != self.__cols:
            raise BaseException('Dimensions of matrix and centers do not match')
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        prefix = 'dist(' + self.__name + ',' + centers.name() + ')';
        dist_job = kmeans_jobs.KMeansDistanceJob(self.__redis, self.__key_mngr, self, centers, prefix)
        
        parts = dist_job.run()
        
        for p in range(0,len(parts)):
            part_name = parts[p]
            m = self.__redis.lpop(part_name)
            sum = None
            while m != None:
                if sum == None:
                    sum = numpy.loads(m)
                else:
                    sum += numpy.loads(m)
                m = self.__redis.lpop(part_name)
                
            self.__redis.delete(part_name)
            redwrap.create_block(self.__key_mngr.get_block_name(result_name, p, 0), numpy.sqrt(sum))
        
        res = Matrix(self.__rows, centers.dimension()[0], result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def k_means_recalc(self, dist, result_name=None):
        """
            Calculates new k-means centers from a previously computed distance matrix
        """
        if result_name == None:
            result_name = MatrixFactory.getRandomMatrixName()
        
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        num_centers = dist.dimension()[1]
        prefix = 'center(' + self.__name + ',' + dist.name() + ')'
        cnt_prefix = 'counter(' + self.__name + ',' + dist.name() + ')_'
        
        recalc_job = kmeans_jobs.KMeansRecalculationJob(self.__redis, self.__key_mngr, self, dist, prefix, cnt_prefix)
        recalc_job.run()
        
        for col in range(0, self.col_blocks()):
            conc = None
            for center in range(0, num_centers):
                name = prefix + '_' + str(col) + '_' + str(center)
                m = self.__redis.lpop(name)
                sum = None
                while m != None:
                    if sum == None:
                        sum = numpy.loads(m)
                    else:
                        sum += numpy.loads(m)
                    m = self.__redis.lpop(name)
                self.__redis.delete(name)
                # Sum is only a row. To make it a matrix that we can concatenate, we have to wrap it
                if len(sum.shape) == 1:
                    sum = numpy.matrix([sum])
                num_records = self.__redis.get(cnt_prefix + str(center))
                num_records = float(num_records) if num_records != None else 1 #TODO: This is an error
                if conc == None:
                    conc = sum / num_records
                else:
                    conc = numpy.concatenate((conc, sum / num_records), axis=0)

            redwrap.create_block(self.__key_mngr.get_block_name(result_name, 0, col), conc)
        res = Matrix(num_centers, self.__cols, result_name, self.__block_size, self.__redis, self.__key_mngr, True)
        return res
    
    def equals(self, m):
        """
            Checks if two matrices contain the same values
        """
        if self.dimension() != m.dimension():
            return False

        equals_job = matrix_jobs.EqualJob(self.__redis, self.__key_mngr, self, m)
        equals_job.run()
        
        pipe = self.__redis.pipeline()
        for i in range(self.row_blocks() * self.col_blocks()):
            pipe.lpop(result_key)
        for val in pipe.execute():
            if int(val) == 0:
                self.__redis.delete(result_key)
                return False
        return True
    
    def count(self):
        """
            For each occurring value per column this method counts the number of occurrences
        """
        prefix = 'count(' + self.name() + ')_'
        count_job = matrix_jobs.CountJob(self.__redis, self.__key_mngr, self, prefix)
        count_job.run()
        
        output = []
        for col in range(0, self.col_blocks()):
            max = self.__block_size
            if col == self.col_blocks()-1 and self.__cols % self.__block_size > 0:
                max = self.__cols % self.__block_size
            
            for i in range(0, max):
                dict = {}
                key = prefix + str(col) + ':' + str(i)
                values = self.__redis.smembers(key)
                for v in values:
                    k = key + ':' + v
                    count = int(self.__redis.get(k))
                    dict[v] = count
                    self.__redis.delete(k)
                output.append(dict)
                self.__redis.delete(key)
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
        
    def toScalar(self):
        """
            Returns a scalar if the matrix consists of only one cell
        """
        redwrap = RedisWrapper(self.__redis, self.__key_mngr)
        if self.dimension()[0] != 1 or self.dimension()[1] != 1:
            raise exceptions.MatrixOperationException('Cannot convert a matrix with more than one column and row to a scalar', 'MATRIX2SCALAR')
        return redwrap.get_block(self.block_name(0,0))[0,0]
    
    def __check_blocksize(self, m):
        if self.block_size() != m.block_size():
            raise Exception("Block sizes do not match")
    
    def __build_modifier_string(self, bools, modifiers):
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
    
    def __str__(self):
        return str(self.get_numpy_matrix())
        
    def __repr__(self):
        return self.__str__()