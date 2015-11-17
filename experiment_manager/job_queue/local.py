
from . import JobQueue
import pip

class LocalJobQueue(JobQueue):
	def __init__(self, **kwargs):
		super(LocalJobQueue, self).__init__(**kwargs)

	def submit_job(self, job):
		job.status = 'running'
		job.run()
		job.status = 'done'

	def check_job(self, job):
		if job.status == 'running':
			job.status = 'unfinished'

	def set_virtualenv(self, virtual_env, requirements=[]):
		if not isinstance(requirements,list):
			requirements = [requirements]
		for package in requirements:
			pip.main(['install', package])

	def update_virtualenv(self, virtual_env, requirements=[]):
		if not isinstance(requirements,list):
			requirements = [requirements]
		for package in requirements:
			pip.main(['install', '--upgrade', package])