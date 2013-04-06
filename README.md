RedisML
=======

A project for parallelizing matrix operations using redis and numpy

The project aims at providing parallel execution of matrix operations such as multiplication,
cell-wise operations, matrix-scalar operations, row and column sums, etc.

General explanation
-----------------------
A matrix is split into blocks and then stored in the redis server. All operations on matrices that can be parallelized are called "jobs".
These jobs have subjobs that are pushed to the workers and instructs them to execute a certain operation on one or more blocks.
A matrix is split into blocks and then stored on several redis instances called "slaves". There is also a master redis instance which is used for storing management information. The master can at the same time be a slave, which means it also stores matrix blocks.
All operations on matrices that can be parallelized are called "jobs". These jobs have subjobs that are pushed to the workers and each subjob works on a number of matrix blocks.

Workers that want to participate in the calculations listen to new subjobs being pushed into a list in the key "free_jobs" on the master redis instance.
Once a new subjob is added, the first worker to retrieve it gets the complete job information from a key that can be computed from the job id in the list.
The subjob command contains information about which blocks to use, under which key to store the result and which operation to perform.
After the worker has sucessfully executed the command it uses redis' publish/subscribe functionality to inform the server.

First steps
-----------------------
This assumes you are running the server and all workers on localhost. This is the only practical thing at the moment since I have not yet implemented a nice configuration.


1. Download redis from http://redis.io/download
2. Add the RedisML project to your PYTHONPATH variable or the redisml directory into your Python lib folder
3. Run the client script with the command "python worker.py sample_server"
4. Go to tests and run python matrix_tests.py

Configuration
-----------------------
The server is configured using a file in json format. It can look as follows:
<code>
<pre>
{
    "server_name" : "sample_server",
    "matrix" :
    {
        "block_size"  : 128
    },
    "redis_master" :
    {
        "host"    : "localhost",
        "port"    : 6379,
        "db"      : 0
    },
    "redis_slaves" :
    {
        "slave1"  : { "host" : "localhost", "port" : 6379, "db" : 1 },
        "slave2"  : { "host" : "localhost", "port" : 6379, "db" : 2 }
    }
}
</pre>
</code>

Currently the configuration consists of four parts:
* The server name is used to distinguish several redisml servers that use the same redis instance as a master.
* The matrix dictionary contains information about the matrices that are created with this server. Currently this is only the block size.
* "redis_master" contains information about which redis instance is used as a redis master. This is where all the management information about jobs and matrices is stored.
* "redis_slaves" contains all redis instances that are used for storing matrix blocks. In this example blocks are distributed over 2 databases on the same redis instance.
