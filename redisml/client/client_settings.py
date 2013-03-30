import logging

COMMAND_MAPPING = {

    # Matrix-Matrix operations
    'MMULT'        : 'redisml.client.commands.mmult',
    'MADD'         : 'redisml.client.commands.madd',
    'DIST'         : 'redisml.client.commands.k_means_distance',
    'RECALC'       : 'redisml.client.commands.k_means_recalc',
    'CW'           : 'redisml.client.commands.cw',
    # Matrix-Scalar operations
    'MS'           : 'redisml.client.commands.ms',
    # Matrix creation
    'MRAND'        : 'redisml.client.commands.mrand',
    'MONES'        : 'redisml.client.commands.mones',
    'MZERO'        : 'redisml.client.commands.mzeros',
    # Unary operations
    'MTRANS'       : 'redisml.client.commands.mtrans',
    'MTRACE'       : 'redisml.client.commands.mtrace',
    'MSUM'         : 'redisml.client.commands.msum',
    'ROWSUM'       : 'redisml.client.commands.rowsum',
    'COLSUM'       : 'redisml.client.commands.colsum',
    # Misc
    'DEL'          : 'redisml.client.commands.delete',
    'EQUAL'        : 'redisml.client.commands.equal',
    'COUNT'        : 'redisml.client.commands.count',
    # Debug
    'FAIL'         : 'redisml.client.commands.fail',
    'TIMEOUT'      : 'redisml.client.commands.timeout'

}

def initLogging():
    logging.basicConfig(level=logging.DEBUG)