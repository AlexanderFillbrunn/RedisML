from redisml.worker.management.base import Worker
from redisml.worker import worker_settings
import redis
import sys

host = 'localhost'
port = 6379
db = 0

if len(sys.argv) > 2:
    for i in range(1,len(sys.argv),2):
        if sys.argv[i] == 'h' and i <= len(sys.argv)-2:
            host = sys.argv[i+1]
        if sys.argv[i] == 'p' and i <= len(sys.argv)-2:
            port = int(sys.argv[i+1])
        if sys.argv[i] == 'db' and i <= len(sys.argv)-2:
            db = int(sys.argv[i+1])

worker_settings.initLogging()
c = Worker(host, port, db)
c.run()