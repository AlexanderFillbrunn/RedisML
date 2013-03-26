RedisML
=======

A project for parallelizing matrix operations using redis and numpy

The project aims at providing parallel execution of matrix operations such as multiplication,
cell-wise operations, matrix-scalar operations, row and column sums, etc.

General explanation
A matrix is splitted into blocks and then stored in the redis server. All operations on matrices that can be parallelized are called "jobs".
These jobs have subjobs that are pushed to the clients/workers and each subjob works on a number of matrix blocks.
Computers that want to participate in the calculations listen to new subjobs being pushed into a list in the key "free_jobs".
Once a new subjob is added, the first client to retrieve it
gets the complete job information from a key that can be computed from the job id in the list.
The subjob command contains information about which blocks to use and which operation to perform.
The client executes the command and uses redis' publish/subscribe functionality to inform the server.
