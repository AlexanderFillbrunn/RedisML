import logging, sys
MAX_JOB_EXECUTIONS = 3
DEFAULT_JOB_TIMEOUT = 0 # Timeout in seconds

# initialize the logger
job_logger = logging.getLogger('jobs')
job_logger.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
job_logger.addHandler(ch)