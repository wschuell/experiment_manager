
from . import BaseJobQueue
import pip

class LocalJobQueue(BaseJobQueue):
	def __init__(self):
		super(LocalJobQueue, self).__init__()

	def submit_job(self, job):
		job.status = 'running'
		job.run()
		job.status = 'done'

	def check_job(self, job):
		if job.status == 'running':
			job.status = 'unfinished'

	def set_virtualenv(self, virtual_env, requirements):
		for package in requirements:
			pip.main(['install', package])

	def update_virtualenv(self, virtual_env, requirements=[]):
		for package in requirements:
			pip.main(['install', '--upgrade', package])