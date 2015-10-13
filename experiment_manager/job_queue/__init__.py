#partly based on Thibaut Munzer's script

import time
from importlib import import_module
from .job import Job


job_queue_class={
	'local':'local.LocalJobQueue',
	'local_multiprocess':'local.MultiProcessJobQueue',
	'avakas':'avakas.AvakasJobQueue',
	'plafrim':'plafrim.PlafrimJobQueue'
}

def JobQueue(jq_type='local', **jq_cfg2):
	tempstr = jq_type
	if tempstr in job_queue_class.keys():
		tempstr = job_queue_class[tempstr]
	templist = tempstr.split('.')
	temppath = '.'.join(templist[:-1])
	tempclass = templist[-1]
	_tempmod = import_module('.'+temppath,package=__name__)
	return getattr(_tempmod,tempclass)(**jq_cfg2)


class BaseJobQueue(object):
	def __init__(self):
		self.job_list = []

	def add_job(self, job):
		job.status = 'pending'
		self.job_list.append(job)

	def update_queue(self):
		for j in self.job_list:
			if j.status == 'pending':
				self.submit_job(j)
			elif j.status == 'running':
				if not self.check_job_running(j):
					self.retrieve_data(job)
			if j.status == 'unfinished':
				j.fix()
				j.status == 'pending'
			elif j.status == 'done':
				self.job_list.remove(j)
			elif self.status == 'missubmitted':
				print('Missubmitted job: '+'_'.join(job.descr,job.uuid))

	def auto_finish_queue(self,t=60):
		if not job_list == []:
			self.update_queue()
			time.sleep(t)
			self.auto_finish_queue(t=t)

	def submit_job(self, job):
		pass

	def check_job_running(self, job):
		pass

	def set_virtualenv(self, virtual_env, requirements):
		pass

	def update_virtualenv(self, virtual_env, requirements):
		pass

	def check_virtualenvs(self):
		envs = []
		for j in self.job_list:
			env = str(j.virtual_env)
			if env not in envs:
				self.update_virtualenv(env)
				envs.append(env)

	def retrieve_data(self, job):
		pass

