RedisML
=======

A project for parallelizing matrix operations using redis and numpy

The project aims at providing parallel execution of matrix operations such as multiplication,
cell-wise operations, matrix-scalar operations, row and column sums, etc.

General explanation
-----------------------
Matrices in RedisML are not stored in one piece. Instead, it is divided into blocks of a certain size and each block is stored under a unique key on a redis database.
A matrix does not even need to be stored on a single redis instance. RedisML can be configured to have one master redis instance that handles the communication and bookkeeping and several slave instances that only store matrix blocks. This sharding method is a substitute for [redis cluster](http://redis.io/topics/cluster-spec), which is currently under development. 
When an operation is performed on a matrix it can often be divided into multiple operations on different blocks. When there are multiple workers connected, each can process a part of an operation by performing it only on certain blocks.
An operation that can be parallelized in RedisML is a job. Such a job has subjobs that are pushed to the workers and instructs them to execute a certain operation on one or more blocks.

Workers that want to participate in the calculations listen to new subjobs being pushed into a list in the key "free_jobs" on the master redis instance.
Once a new subjob is added, the first worker to retrieve it gets the complete job information from a key that can be computed from the job id in the list.
The subjob command contains information about which blocks to use, under which key to store the result and which operation to perform.
After the worker has sucessfully executed the command it uses redis' publish/subscribe functionality to inform the server.

This concept allows parallelization of expensive matrix operations and additionally distribution of large matrices to several redis instances.

<pre>

     +--------------+        +--------------+        +--------------+        +--------------+
     |    Worker    |        |    Worker    |        |    Worker    |        |    Worker    |
     |--------------|        |--------------|        |--------------|        |--------------|
     | o Logic for  |        | o Logic for  |        | o Logic for  |        | o Logic for  |
     |   subjobs    |        |   subjobs    |        |   subjobs    |        |   subjobs    |
     |              |        |              |        |              |        |              |
     +------+-------+        +-------+------+        +-------+------+        +-------+------+
            |                        +--+                    |                       |
            |                           |                    |                       |
            +---------------------------o--------------------+-----------------------+
                                        |
            +---------------------------o-----------------------------+
            |                           |                             |
     +------v------+           +--------v---------+            +------v------+
     | Redis Slave |           |   Redis Master   |            | Redis Slave |
     |-------------|           |------------------|            |-------------|
     | o Matrix    |           | o Housekeeping   |            | o Matrix    |
     |   data      |           |   - Slave info   |            |   data      |
     |             |           |   - Matrix meta  |            |             |
     |             |           |            data  |            |             |
     |             |           |                  |            |             |
     +-------------+           +------------------+            +-------------+
            ^                             ^                            ^
            |                             |                            |
            |                             |                            |
            |                             |                            |
            |                    +--------+-------+                    |
            |                    |     Server     |                    |
            |                    |----------------|                    |
            |                    | o Logic        |                    |
            +--------------------+                +--------------------+
                                 |                |
                                 +----------------+
</pre>

First steps
-----------------------
This assumes you are running the server and all workers on localhost. This is the only practical thing at the moment since I have not yet implemented a nice configuration.


1. Download redis from http://redis.io/download
2. Make sure you have [numpy](http://www.numpy.org), [scipy](http://www.scipy.org) and [redis-py](https://github.com/andymccurdy/redis-py) installed 
3. Add the RedisML project to your PYTHONPATH variable or the redisml directory into your Python lib folder
4. Run the redis server
5. Run the client script with the command "python worker.py sample_server"
6. Go to tests and run python matrix_tests.py

Configuration
-----------------------
The server is configured using a file in json format. It can look as follows:
<code>
<pre>
{
    "server_name" : "sample_server",
    "jobs" :
    {
        "max_execs" : 3,
        "timeout"   : 10
    },
    "matrix" :
    {
        "block_size"  : 256
    },
    "redis_master" :
    {
        "host"    : "localhost",
        "port"    : 6379,
        "db"      : 0
    },
    "redis_slaves" :
    {
        "master"  : { "host" : "localhost", "port" : 6379, "db" : 0 }
    }
}</pre>
</code>

Currently the configuration consists of five parts:
* The server name is used to distinguish several redisml servers that use the same redis instance as a master.
* The "jobs" dictionary contains job configuration values. "max_execs" sets the number of times the server tries to execute a job and "timeout" sets the time in seconds that the server waits for a job to be executed before submitting it again.
* The "matrix" dictionary contains information about the matrices that are created with this server. Currently this is only the block size.
* "redis_master" contains information about which redis instance is used as a redis master. This is where all the management information about jobs and matrices is stored.
* "redis_slaves" contains all redis instances that are used for storing matrix blocks. In this example blocks are distributed over 2 databases on the same redis instance.
