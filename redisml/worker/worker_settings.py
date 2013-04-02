import logging

COMMAND_MAPPING = {

    # Matrix-Matrix operations
    'MMULT'        : 'redisml.worker.commands.mmult',
    'MADD'         : 'redisml.worker.commands.madd',
    'DIST'         : 'redisml.worker.commands.k_means_distance',
    'RECALC'       : 'redisml.worker.commands.k_means_recalc',
    'CW'           : 'redisml.worker.commands.cw',
    # Matrix-Scalar operations
    'MS'           : 'redisml.worker.commands.ms',
    # Matrix creation
    'MRAND'        : 'redisml.worker.commands.mrand',
    'MONES'        : 'redisml.worker.commands.mones',
    'MZERO'        : 'redisml.worker.commands.mzeros',
    # Unary operations
    'MTRANS'       : 'redisml.worker.commands.mtrans',
    'MTRACE'       : 'redisml.worker.commands.mtrace',
    'MSUM'         : 'redisml.worker.commands.msum',
    'ROWSUM'       : 'redisml.worker.commands.rowsum',
    'COLSUM'       : 'redisml.worker.commands.colsum',
    # Misc
    'DEL'          : 'redisml.worker.commands.delete',
    'EQUAL'        : 'redisml.worker.commands.equal',
    'COUNT'        : 'redisml.worker.commands.count',
    # Debug
    'FAIL'         : 'redisml.worker.commands.fail',
    'TIMEOUT'      : 'redisml.worker.commands.timeout'

}

def initLogging():
    logging.basicConfig(level=logging.DEBUG)