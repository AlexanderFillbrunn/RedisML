from redisml.server.jobs import jobs
import redisml.shared.commands as cmd

class UnaryMatrixJob(jobs.Job):
    def __init__(self, context, matrix):
        super(UnaryMatrixJob, self).__init__(context)
        self.matrix = matrix

    def setMatrix(matrix):
        self.matrix = matrix
        
class BinaryMatrixJob(jobs.Job):
    
    def __init__(self, context, matrix1, matrix2):
        super(BinaryMatrixJob, self).__init__(context)
        self.matrix1 = matrix1
        self.matrix2 = matrix2

    def setMatrix1(matrix):
        self.matrix1 = matrix

    def setMatrix2(matrix):
        self.matrix2 = matrix

class MatrixScalarJob(jobs.Job):
    
    def __init__(self, context, matrix, scalar, op, result_name):
        super(MatrixScalarJob, self).__init__(context)
        self.matrix = matrix
        self.scalar = scalar
        self.operation = op
        self.result_name = result_name
        
    def run(self):
        for col in range(0, self.matrix.col_blocks()):
            for row in range(0,self.matrix.row_blocks()):
                mult_cmd = cmd.build_command('MS', str(self.scalar), self.operation,
                                                            self.matrix.block_name(row, col),
                                                            self.context.key_manager.get_block_name(self.result_name, row, col))
                self.add_subjob(mult_cmd)
        self.execute()

class CreateMatrixJob(jobs.Job):
    
    def __init__(self, context, content, rows, cols, result_name):
        super(CreateMatrixJob, self).__init__(context)
        self.content = content
        self.rows = rows
        self.cols = cols
        self.result_name = result_name
        
    def run(self):
        numcols = self.cols / self.context.block_size
        if self.cols % self.context.block_size != 0:
            numcols += 1
        numrows = self.rows / self.context.block_size
        if self.rows % self.context.block_size != 0:
            numrows += 1
            
        for col in range(0, numcols):
            for row in range(0, numrows):
                cols = self.context.block_size
                rows = self.context.block_size
                # In case the matrix size is not a multiple of the block size
                if (row+1) * self.context.block_size > self.rows:
                    rows = self.rows % self.context.block_size
                if (col+1) * self.context.block_size > self.cols:
                    cols = self.cols % self.context.block_size
                comm = cmd.build_command(cmd.CREATE, self.content, rows, cols, self.context.key_manager.get_block_name(self.result_name, row, col))
                self.add_subjob(comm)
        self.execute()


class BinaryExpressionJob(BinaryMatrixJob):

    def __init__(self, context, matrix1, matrix2, expr, result_name):
        super(BinaryExpressionJob, self).__init__(context, matrix1, matrix2)
        self.expr = expr
        self.result_name = result_name
        
    def run(self):
         for col in range(0, self.matrix1.col_blocks()):
            for row in range(0,self.matrix1.row_blocks()):
                comm = cmd.build_command('MBIN', self.matrix1.block_name(row, col), self.matrix2.block_name(row, col), self.expr, 
                                            self.context.key_manager.get_block_name(self.result_name, row, col))
                self.add_subjob(comm)
         self.execute()
     
class TraceJob(UnaryMatrixJob):

    def __init__(self, context, matrix, output_key):
        super(TraceJob, self).__init__(context, matrix)
        self.output_key = output_key
        
    def run(self):
        for k in range(0, self.matrix.row_blocks()):
            trace_cmd = cmd.build_command('MTRACE', self.matrix.block_name(k, k), self.output_key)
            self.add_subjob(trace_cmd)
        self.execute()
        
class CountJob(UnaryMatrixJob):
    
    def __init__(self, context, matrix, output_prefix):
        super(CountJob, self).__init__(context, matrix)
        self.output_prefix = output_prefix
        
    def SetOutputPrefix(self, output_prefix):
        self.output_prefix = output_prefix
        
    def run(self):
        for col in range(0, self.matrix.col_blocks()):
            key = self.output_prefix + str(col)
            for row in range(0, self.matrix.row_blocks()):
                count_cmd = cmd.build_command('COUNT', self.matrix.block_name(row, col), key)
                self.add_subjob(count_cmd)
        self.execute()

class TransposeJob(UnaryMatrixJob):
    
    def __init__(self, context, matrix, result_name):
        super(TransposeJob, self).__init__(context, matrix)
        self.result_name = result_name

    def run(self):
        for col in range(0, self.matrix.col_blocks()):
            for row in range(0,self.matrix.row_blocks()):
                transp_cmd = cmd.build_command('MTRANS',
                                                            self.matrix.block_name(row, col),
                                                            self.context.key_manager.get_block_name(self.result_name, col, row))
                self.add_subjob(transp_cmd)
        self.execute()

class MultiplicationJob(BinaryMatrixJob):
    """
        Job that does the first part of the matrix multiplication: Multiplicating blocks
        The second part is adding the intermediary results with a cell wise add
    """
    def __init__(self, context, matrix1, matrix2, transpose_m1, transpose_m2, negate_m1, negate_m2, result_name_format):
        super(MultiplicationJob, self).__init__(context, matrix1, matrix2)
        self.result_name_format = result_name_format
        self.transpose_m1 = transpose_m1
        self.transpose_m2 = transpose_m2
        self.negate_m1 = negate_m1
        self.negate_m2 = negate_m2
        
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
        
    def run(self):
        # modifiers are used to negate or transpose a matrix "on-the-fly" which saves commands
        modifiers = ["t", "-"]
        mod1 = self.__build_modifier_string([self.transpose_m1, self.negate_m1], modifiers)
        mod2 = self.__build_modifier_string([self.transpose_m2, self.negate_m2], modifiers)
        mod = mod1 + ";" + mod2

        for col in range(0, self.matrix1.col_blocks()):
            for row in range(0,self.matrix1.row_blocks()):
                for col2 in range(0,self.matrix1.row_blocks()):
                    m1_block_name = self.matrix1.block_name(col, row) if self.transpose_m1 else self.matrix1.block_name(row, col)
                    m2_block_name = self.matrix2.block_name(col2, col) if self.transpose_m2 else self.matrix2.block_name(col, col2)
                    mult_cmd = cmd.build_command('MMULT', mod, m1_block_name, m2_block_name,
                                                            self.result_name_format.format(self.context.key_manager.get_slave(col, row, col2), col, row, col2))
                    self.add_subjob(mult_cmd)
        self.execute()

class MultiplicationMergeJob(jobs.Job):
    def __init__(self, context, rows, cols, input_name_format, result_name):
        super(MultiplicationMergeJob, self).__init__(context)
        self.rows = rows
        self.cols = cols
        self.input_name_format = input_name_format
        self.result_name = result_name
        
    def run(self):
        for col in range(0, self.cols):
            for row in range(0,self.rows):
                add_cb = cmd.CommandBuilder('MADD')
                del_cb = cmd.CommandBuilder('DEL')
                for col2 in range(0,self.rows):
                    mname = self.input_name_format.format(self.context.key_manager.get_slave(col2, row, col), col2, row, col)
                    add_cb.add_param(mname)
                    del_cb.add_param(mname)
                    
                add_cb.add_param(self.context.key_manager.get_block_name(self.result_name, row, col))
                self.add_subjob(add_cb.join(del_cb))
        self.execute()

class EqualJob(BinaryMatrixJob):
    
    def __init__(self, context, matrix1, matrix2, result_key):
        super(EqualJob, self).__init__(context, matrix1, matrix2)
        self.result_key = result_key
    
    def run(self):
        for row in range(0, self.matrix1.row_blocks()):
            for col in range(0, self.matrix1.col_blocks()):
                equal_cmd = cmd.build_command('EQUAL', self.matrix1.block_name(row, col), self.matrix2.block_name(row, col), self.result_key)
                self.add_subjob(equal_cmd)
        self.execute()