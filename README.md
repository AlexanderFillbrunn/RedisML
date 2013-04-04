RedisML
=======

A project for parallelizing matrix operations using redis and numpy

The project aims at providing parallel execution of matrix operations such as multiplication,
cell-wise operations, matrix-scalar operations, row and column sums, etc.

General explanation
-----------------------
A matrix is split into blocks and then stored in the redis server. All operations on matrices that can be parallelized are called "jobs".
These jobs have subjobs that are pushed to the workers and each subjob works on a number of matrix blocks.

Computers that want to participate in the calculations listen to new subjobs being pushed into a list in the key "free_jobs".
Once a new subjob is added, the first worker to retrieve it gets the complete job information from a key that can be computed from the job id in the list.
The subjob command contains information about which blocks to use, under which key to store the result and which operation to perform.
After the worker has sucessfully executed the command it uses redis' publish/subscribe functionality to inform the server.

First steps
-----------------------
This assumes you are running the server and all workers on localhost. This is the only practical thing at the moment since I have not yet implemented a nice configuration.


1. Download redis from http://redis.io/download
2. Add the RedisML project to your PYTHONPATH variable or the redisml directory into your Python lib folder
3. Run the client script with python worker.py
4. Go to tests and run python matrix_tests.py
