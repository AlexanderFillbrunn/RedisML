# The key under which the list of executable jobs is accessible
FREE_JOBS_KEY = '{0}:free_jobs'
# The Pub/Sub channel where clients report the results of a job execution
JOB_RESULTS_CHANNEL = '{0}:c_results'
# The key from which the id for a new job can be retrieved
JOB_IDS_KEY = '{0}:job_ids'
# The format for the key under which connection information for a slave is saved
SLAVE_KEY = '{0}:slave:{1}'
# The format for the key under which the list of slaves for a server is saved
SLAVE_LIST_KEY = '{0}:slaves'
# The key under which a list of successfully executed jobs is saved
FINISHED_JOBS_KEY = '{0}:finished_jobs'
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