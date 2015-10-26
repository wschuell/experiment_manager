#partly based on Thibaut Munzer's script

import os
import time
import cPickle
import uuid
from importlib import import_module
from ..job import Job
import copy


job_queue_class={
	'local':'local.LocalJobQueue',
	'local_multiprocess':'local.MultiProcessJobQueue',
	'avakas':'avakas.AvakasJobQueue',
	'plafrim':'plafrim.PlafrimJobQueue'
}

def get_jobqueue(jq_type='local', name =None, **jq_cfg2):
	if name is not None and os.path.isfile('jobs/'+name+'.jq'):
		with open('jobs/'+name+'.jq','r') as f:
			jq = cPickle.loads(f.read())
	tempstr = jq_type
	if tempstr in job_queue_class.keys():
		tempstr = job_queue_class[tempstr]
	templist = tempstr.split('.')
	temppath = '.'.join(templist[:-1])
	tempclass = templist[-1]
	_tempmod = import_module('.'+temppath,package=__name__)
	return getattr(_tempmod,tempclass)(name=name, **jq_cfg2)


class JobQueue(object):
	def __init__(self, erase=True, auto_update=True, name=None):
		self.job_list = []
		self.erase = erase
		self.auto_update = auto_update
		self.past_exec_time = 0
		if name is None:
			self.name = str(uuid.uuid1())
		else:
			self.name = name

	def save(self):
		with open('jobs/'+self.name+'.jq','w') as f:
			f.write(cPickle.dumps(self,cPickle.HIGHEST_PROTOCOL))

	def add_job(self, job):
		eq_filter = [j for j in self.job_list if (j == job)]
		lt_filter = [j for j in eq_filter if eq_filter and (j < job)]
		if not eq_filter:
			self.status = 'pending'
			self.job_list.append(job)
		elif lt_filter:
			self.status = 'dependencies not satisfied'
		else:
			print 'Job already in queue!'
		job.save()
		self.save()

	def update_queue(self):
		if self.auto_update:
			self.check_virtualenvs()
		for j in [x for x in self.job_list]:
			if j.status == 'dependencies not satisfied':
				j.re_init()
				self.job_list.remove(j)
				self.add_job(j)
			if j.status == 'pending' and self.avail_workers()>0:
				self.submit_job(j)
				j.save()
			elif j.status == 'running':
				if not self.check_job_running(j):
					self.retrieve_job(j)
					if j.status == 'pending':
						j.status = 'missubmitted'
			elif j.status == 'dependencies not satisfied':
				for dep in j.gen_depend():
					print 'Adding dependency for job ' + j.job_dir
					self.add_job(dep)
			if j.status == 'unfinished':
				j.fix()
			elif j.status == 'done':
				os.chdir(j.get_path())
				j.get_data()
				os.chdir(j.get_back_path())
				j.unpack_data()
				j.data = None
				self.past_exec_time += j.exec_time
				self.job_list.remove(j)
				if (not self.erase) and (not j.erase):
					j.clean()
			elif j.status == 'missubmitted':
				print('Missubmitted job: '+'_'.join([j.descr,j.uuid]))
			elif j.status == 'dependencies not satisfied':
				print('Dependencies not satisfied for job: '+j.job_dir)
			self.save()

	def auto_finish_queue(self,t=60):
		while job_list:
			self.update_queue()
			print 'Queue updated'
			time.sleep(t)

	def check_virtualenvs(self):
		envs = {}
		for j in self.job_list:
			env = str(j.virtual_env)
			if env not in envs.keys():
				envs[env] = copy.deepcopy(j.requirements)
			else:
				envs[env] += copy.deepcopy(j.requirements)
		for env in envs.keys():
			self.update_virtualenv(env, requirements=list(set(envs[env])))

	def cancel_job(self, job, clean=False):
		if clean:
			job.clean()
		else:
			job.status = 'canceled'
			job.save()

	def total_time(self):
		t = 0
		for j in job_list:
			t += j.estimated_time

	def exec_time(self):
		t = self.past_exec_time
		for j in job_list:
			t += j.exec_time

	def submit_job(self, job):
		pass

	def check_job_running(self, job):
		pass

	def set_virtualenv(self, virtual_env, requirements):
		pass

	def update_virtualenv(self, virtual_env, requirements):
		pass

	def retrieve_job(self, job):
		pass

	def avail_workers(self):
		return 1
