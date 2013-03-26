from redisml.server.jobs import jobs
import redisml.server.command_builder as command_builder
import redisml.server

class KMeansDistanceJob(jobs.Job):

	def __init__(self, redis, data, centers, output_prefix):
		super(KMeansDistanceJob, self).__init__(redis)
		self.data = data
		self.centers = centers
		self.output_prefix = output_prefix
		
	def run(self):
		parts = []
		for row in range(0, self.data.row_blocks()):
			name = self.output_prefix + '_' + str(row)
			parts.append(name)
			for col in range(0, self.data.col_blocks()):
				dist_cmd = command_builder.CommandBuilder('DIST')
				dist_cmd.add_param(self.data.block_name(row, col))
				dist_cmd.add_param(self.centers.block_name(0, col))
				dist_cmd.add_param(name)
				self.add_subjob(dist_cmd.getCmdString())
		self.execute()
		return parts
		
class KMeansRecalculationJob(jobs.Job):
	
	def __init__(self, redis, data, distance_matrix, center_prefix, count_prefix):
		super(KMeansRecalculationJob, self).__init__(redis)
		self.data = data
		self.distance_matrix = distance_matrix
		self.center_prefix = center_prefix
		self.count_prefix = count_prefix
		
	def run(self):
		num_centers = self.distance_matrix.dimension()[1]
		for row in range(0, self.data.row_blocks()):
			recalc_cmd = command_builder.build_command('RECALC', self.data.block_name(row, 0),
																 self.distance_matrix.block_name(row, 0),
																 self.center_prefix + '_0_',
																 self.count_prefix)
			self.add_subjob(recalc_cmd)
			for col in range(1, self.data.col_blocks()):
				recalc_cmd = command_builder.build_command('RECALC', self.data.block_name(row, col),
																	 self.distance_matrix.block_name(row, 0),
																	 self.center_prefix + '_' + str(col) + '_')
				self.add_subjob(recalc_cmd)
		self.execute()