import logging
import redisml.shared.commands as cmd

COMMAND_MAPPING = {

    # Matrix-Matrix operations
    cmd.MATRIXMULT        : 'redisml.worker.commands.mmult',
    cmd.MATRIXADDITION    : 'redisml.worker.commands.madd',
    cmd.KMEANSDIST        : 'redisml.worker.commands.k_means_distance',
    cmd.KMEANSRECALC      : 'redisml.worker.commands.k_means_recalc',
    cmd.BINARYMATRIXOP    : 'redisml.worker.commands.mbin',
    # Matrix-Scalar operations
    cmd.MATRIXSCALAROP    : 'redisml.worker.commands.ms',
    # Matrix creation
    cmd.CREATE            : 'redisml.worker.commands.mcreate',
    # Unary operations
    cmd.TRANSPOSE         : 'redisml.worker.commands.mtrans',
    cmd.TRACE             : 'redisml.worker.commands.mtrace',
    cmd.AGGREGATEOP       : 'redisml.worker.commands.maggr',
    cmd.COUNT             : 'redisml.worker.commands.count',
    # Misc
    cmd.DELETE            : 'redisml.worker.commands.delete',
    cmd.EQUALS            : 'redisml.worker.commands.equal',
    # Debug
    cmd.FAIL              : 'redisml.worker.commands.fail',
    cmd.TIMEOUT           : 'redisml.worker.commands.timeout'

}

def initLogging():
    logging.basicConfig(level=logging.DEBUG)