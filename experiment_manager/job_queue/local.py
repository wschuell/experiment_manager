
from . import BaseJobQueue

class LocalJobQueue(BaseJobQueue)
	def __init__(self):
		super(self,LocalJobQueue).__init__()

	def submit_job(self, job):
		job.status = 'running'
		job.run()
		job.status = 'done'

	def check_job(self, job):
		if job.status == 'running':
			job.status = 'unfinished'

	def check_requirements(self, requirements):
		pass

	def update_requirements(self, requirements):
		pass

	def retrieve_data(self, job):
		pass
