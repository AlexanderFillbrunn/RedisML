
COMMAND_MAPPING = {

	# Matrix-Matrix operations
	'MMULT'		: 'redisml.client.commands.mmult',
	'MADD'		: 'redisml.client.commands.madd',
	# Matrix-Scalar operations
	'MSMULT'	: 'redisml.client.commands.msmult',
	'MSDIV'		: 'redisml.client.commands.msdiv',
	'MSADD'		: 'redisml.client.commands.msadd',
	'MSSUB'		: 'redisml.client.commands.mssub',
	# Matrix creation
	'MRAND'		: 'redisml.client.commands.mrand',
	'MONES'		: 'redisml.client.commands.mones',
	'MZERO'		: 'redisml.client.commands.mzeros',
	# Unary operations
	'MTRANS'	: 'redisml.client.commands.mtrans',
	'MTRACE'	: 'redisml.client.commands.mtrace',
	'MSUM'		: 'redisml.client.commands.msum',
	'MRSUM'		: 'redisml.client.commands.mrsum',
	'MCSUM'		: 'redisml.client.commands.mcsum'

}
