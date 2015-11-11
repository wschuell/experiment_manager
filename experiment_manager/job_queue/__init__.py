#partly based on Thibaut Munzer's script

import os
import time
import cPickle
import uuid
from importlib import import_module
from ..job import Job
import copy
import path


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
	else:
		tempstr = jq_type
		if tempstr in job_queue_class.keys():
			tempstr = job_queue_class[tempstr]
		templist = tempstr.split('.')
		temppath = '.'.join(templist[:-1])
		tempclass = templist[-1]
		_tempmod = import_module('.'+temppath,package=__name__)
		jq = getattr(_tempmod,tempclass)(name=name, **jq_cfg2)
	return jq


class JobQueue(object):
	def __init__(self, erase=True, auto_update=True, name=None, deep_check=False):
		self.job_list = []
		self.erase = erase
		self.auto_update = auto_update
		self.past_exec_time = 0
		self.uuid = str(uuid.uuid1())
		if name is None:
			self.name = self.uuid
		else:
			self.name = name
		self.deep_check = deep_check
		self.executed_jobs = 0

	def save(self):
		with open('jobs/'+self.name+'.jq','w') as f:
			f.write(cPickle.dumps(self,cPickle.HIGHEST_PROTOCOL))

	def add_job(self, job, deep_check=None):
		if job.status == 'dependencies not satisfied':
			deps = job.gen_depend()
			for d in deps:
				uuid_l = self.add_job(d)
				job.deps += uuid_l
		eq_filter = [j for j in self.job_list if (j == job)]
		lt_filter = [j for j in eq_filter if eq_filter and (j < job)]
		ge_filter = [j for j in eq_filter if eq_filter and (j >= job)]
		if not eq_filter:
			self.job_list.append(job)
			self.update_needed = True
			#job.status = 'pending'
			job.save()
			ans = [job.uuid]
		elif lt_filter and not ge_filter:
			job.status = 'dependencies not satisfied'
			self.job_list.append(job)
			self.update_needed = True
			job.deps += [jj.uuid for jj in lt_filter]
			#job.status = 'pending'
			job.save()
			ans = [job.uuid]
		else:
			print 'Job already in queue!'
			job.clean()
			ans = [jj.uuid for jj in eq_filter]
		self.save()
		return ans

	def update_queue(self):
		if self.auto_update and self.update_needed:
			self.check_virtualenvs()
			self.update_needed = False
		for j in [x for x in self.job_list]:
			if j.status == 'dependencies not satisfied':
				job_uuids = [jj.uuid for jj in self.job_list]
				for x in [x for x in j.deps]:
					if x not in job_uuids:
						j.deps.remove(x)
				if not j.deps:
					j.re_init()
				#self.job_list.remove(j)
				#self.add_job(j)
			if j.status == 'pending' and self.avail_workers()>0:
				self.submit_job(j)
				j.save()
			elif j.status == 'running':
				if not self.check_job_running(j):
					self.retrieve_job(j)
					if j.status == 'pending':
						j.status = 'missubmitted'
			#elif j.status == 'dependencies not satisfied':
				#for dep in j.gen_depend():
				#	print 'Adding dependency for job ' + j.job_dir
				#	self.add_job(dep)
			if j.status == 'unfinished':
				j.fix()
			elif j.status == 'done':
				with path.Path(j.get_path()):
					j.get_data()
				j.unpack_data()
				j.data = None
				self.past_exec_time += j.exec_time
				self.executed_jobs += 1
				self.job_list.remove(j)
				if (not self.erase) and (not j.erase):
					j.clean()
			elif j.status == 'missubmitted':
				print('Missubmitted job: '+'_'.join([j.descr,j.uuid]))
			elif j.status == 'dependencies not satisfied':
				print('Dependencies not satisfied for job: '+j.job_dir)
			self.save()

	def auto_finish_queue(self,t=60):
		self.update_queue()
		print time.gmtime(),' Queue updated'
		while [j for j in self.job_list if (j.status != 'missubmitted' and j.status != 'dependencies not satisfied')]:
			self.update_queue()
			print time.gmtime(),' Queue updated'
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
		for j in self.job_list:
			t += j.estimated_time

	def exec_time(self):
		t = self.past_exec_time
		for j in self.job_list:
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
