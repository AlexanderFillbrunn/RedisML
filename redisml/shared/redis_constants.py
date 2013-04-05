# The key under which the list of executable jobs is accessible
FREE_JOBS_KEY = 'free_jobs'
# The Pub/Sub channel where clients report the results of a job execution
JOB_RESULTS_CHANNEL = 'c_results'
# The key from which the id for a new job can be retrieved
JOB_IDS_KEY = 'job_ids'
# The format of block names
BLOCK_NAME_FORMAT = '{0}:{1}:{2}_{3}'
# The format of the key where matrix information is saved
INFO_FORMAT         = '{0}:info'
# The format of a matrix name, where argument 0 is a counter and argument 1 a random number
MATRIX_NAME_FORMAT     = 'matrix{0}_{1}'

####### Helper functions #######

def get_slave_name(block_name):
    return block_name.split(":")[1]
    
def get_block_name(matrix_name, slave_name, row, col):
    return BLOCK_NAME_FORMAT.format(matrix_name, slave_name, row, col)