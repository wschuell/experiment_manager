#partly based on Thibaut Munzer's script

import os
import sys
import time
try:
	import cPickle as pickle
except ImportError:
	import pickle
import uuid
from importlib import import_module
from ..job import Job
import copy
import path
import errno
try:
	from IPython.display import clear_output as cl_output
except:
	pass

job_queue_class={
	'local':'local.LocalJobQueue',
	'local_multiprocess':'local.LocalMultiProcessJobQueue',
	'avakas':'avakas.AvakasJobQueue',
	'plafrim':'plafrim.PlafrimJobQueue',
	'plafrim_oldslurm':'plafrim.PlafrimOldSlurm',
	'torque':'torque.TorqueJobQueue',
	'slurm':'slurm.SlurmJobQueue',
	'dockerslurm':'docker.DockerSlurmJobQueue',
	'oldslurm':'slurm.OldSlurmJobQueue',
	'anyone':'anyone.AnyoneJobQueue',
	'anyone_oldslurm':'anyone.AnyoneOldSlurm'
}

def get_jobqueue(jq_type='local', name =None, **jq_cfg2):
	if name is not None and os.path.isfile('job_queues/'+name+'.jq'):
		with open('job_queues/'+name+'.jq','rb') as f:
			jq = pickle.loads(f.read())
			if 'db' in list(jq_cfg2.keys()):
				jq.db = jq_cfg2['db']
	else:
		tempstr = jq_type
		if tempstr in list(job_queue_class.keys()):
			tempstr = job_queue_class[tempstr]
		templist = tempstr.split('.')
		temppath = '.'.join(templist[:-1])
		tempclass = templist[-1]
		_tempmod = import_module('.'+temppath,package=__name__)
		jq = getattr(_tempmod,tempclass)(name=name, **jq_cfg2)
	return jq


class JobQueue(object):
	def __init__(self, erase=False, auto_update=True, virtual_env = None, requirements = [], name=None, deep_check=False, verbose=False,path='job_queues/', reinit_missubmitted_times=0, py3_suffix=True):
		self.verbose = verbose
		self.job_list = []
		self.erase = erase
		self.update_needed = False
		self.auto_update = auto_update
		self.past_exec_time = 0
		self.uuid = str(uuid.uuid1())
		if name is None:
			self.name = self.uuid
		else:
			self.name = name
		self.deep_check = deep_check
		self.executed_jobs = 0
		self.reinit_missubmitted_times = reinit_missubmitted_times
		self.jobqueue_dir = '_'.join([time.strftime('%Y-%m-%d_%H-%M-%S'), self.__class__.__name__, self.uuid])
		self.path = os.path.join(path,self.jobqueue_dir)
		self.original_path = path
		self.virtual_env = virtual_env
		self.requirements = requirements
		self.python_version = sys.version_info[0]

	def save(self):
		if not os.path.isdir(self.path):#'jobs'):
			os.makedirs(self.path)#'jobs')
		with open(os.path.join(self.path,self.name+'.jq'),'wb') as f:#'jobs/'+self.name+'.jq','w') as f:
			f.write(pickle.dumps(self,pickle.HIGHEST_PROTOCOL))
		try:
			os.symlink(os.path.join(self.jobqueue_dir,self.name+'.jq'),os.path.join(self.original_path,self.name+'.jq'))
		except OSError as e:
			if e.errno == errno.EEXIST:
				os.remove(os.path.join(self.original_path,self.name+'.jq'))
				os.symlink(os.path.join(self.jobqueue_dir,self.name+'.jq'),os.path.join(self.original_path,self.name+'.jq'))

	def check_backups(self):
		self.backups_status = {'present':[],'locked':[]}

	def add_job(self, job, deep_check=None,save=True):
		if job.status == 'already done':
			job.clean()
			return []
		elif job.status == 'dependencies not satisfied':
			deps = job.gen_depend()
			for d in deps:
				uuid_l = self.add_job(d,save=False)
				job.deps += uuid_l
		eq_filter = [j for j in self.job_list if (j == job)]
		lt_filter = [j for j in eq_filter if (j < job)]
		ge_filter = [j for j in eq_filter if (j >= job)]
		if not eq_filter:
			self.append_job(job)
			#self.job_list.append(job)
			#self.move_job(job)
			#job.reinit_missubmitted_times = self.reinit_missubmitted_times
			#self.update_needed = True
			#job.status = 'pending'
			job.save()
			ans = [job.uuid]
		elif lt_filter and not ge_filter:
			job.status = 'dependencies not satisfied'
			self.append_job(job)
			#self.job_list.append(job)
			#self.move_job(job)
			#self.update_needed = True
			job.deps += [jj.uuid for jj in lt_filter]
			#job.status = 'pending'
			job.save()
			ans = [job.uuid]
		else:
			if self.verbose:
				print('Job already in queue!')
			job.clean()
			ans = [jj.uuid for jj in eq_filter]
		if save:
			self.save()
		job.close_connections()
		return ans

	def append_job(self,job):
		self.job_list.append(job)
		self.move_job(job)
		self.update_needed = True
		job.reinit_missubmitted_times = self.reinit_missubmitted_times
		if job.virtual_env is None and hasattr(self,'virtual_env'):
			job.virtual_env = self.virtual_env

	def move_job(self,job):
		if job.init_path in ['jobs','jobs/']:
			rm = True
		else:
			rm = False
		job.move(new_path=self.path)
		if rm:
			try:
				os.rmdir('jobs')
			except OSError:
				pass

	def update_queue(self,clear_output=False):
		if hasattr(self,'last_update') and time.time() - self.last_update < 1:
			time.sleep(1)
		self.save_status(message='Starting queue update')
		if self.auto_update and self.update_needed:
			self.check_virtualenvs()
			self.update_needed = False
			self.save_status(message='Requirements installed')
			self.save()
		self.check_running_jobs()
		self.check_backups()

		for j in [x for x in self.job_list]:
			if j.status == 'finished running':
				self.retrieve_job(j)
				if j.status in ['pending','running']:
					j.status = 'missubmitted'
			#elif j.status == 'dependencies not satisfied':
				#for dep in j.gen_depend():
				#	print('Adding dependency for job ' + j.job_dir)
				#	self.add_job(dep)
			j.close_connections()

		retrieved_list = self.global_retrieval()
		for j in retrieved_list:
			if j.status in ['pending','running']:
					j.status = 'missubmitted'

		self.reinit_missubmitted()

		for j in [x for x in self.job_list]:
			if j.status == 'unfinished':
				j.fix()
				if hasattr(self, 'extended_jobs'):
					self.extended_jobs += 1
				else:
					self.extended_jobs = 1
			elif j.status == 'done':
				if j.get_data_at_unpack:
					with path.Path(j.get_path()):
						j.get_data()
				j.unpack_data()
				j.data = None
				self.past_exec_time += j.exec_time
				self.executed_jobs += 1
				#if self.erase:
				j.status = 'to be cleaned'
			j.close_connections()

		for j in [x for x in self.job_list]:
			if j.status == 'dependencies not satisfied':
				job_uuids = [jj.uuid for jj in self.job_list if jj.status not in ['done','to be cleaned']] # maybe manage dependencies differently: stay in 'done' status , or 'unpacked', and clean only if deps do not need it anymore
				for dep_uuid in [dep_uuid for dep_uuid in j.deps]:
					if dep_uuid not in job_uuids:
						j.deps.remove(dep_uuid)
				if not j.deps:
					j.re_init()
				#self.job_list.remove(j)
				#self.add_job(j)
			j.close_connections()

		for j in [x for x in self.job_list]:
			if j.status == 'pending' and self.avail_workers()>0:
				j.save()
				self.submit_job(j)
				j.save()
			j.close_connections()

		self.global_submit()

		for j in [x for x in self.job_list]:
			if j.status == 'to be cleaned':
				self.job_list.remove(j)
				if self.erase:
					j.clean()
			elif self.verbose and j.status == 'missubmitted':
				print('Missubmitted job: '+'_'.join([j.descr,j.uuid]))
			elif self.verbose and j.status == 'dependencies not satisfied':
				print('Dependencies not satisfied for job: '+j.job_dir)
			elif self.verbose and j.status == 'script error':
				print('Script error for job: '+j.job_dir)
			j.close_connections()

		self.save()
		if clear_output:
			try:
				cl_output(wait=True)
			except:
				pass
		print(self.get_status_string())
		self.save_status()
		if self.job_list and not [j for j in self.job_list if j.status not in ['missubmitted', 'script error', 'dependencies not satisfied']]:
			raise Exception('Queue blocked, only missubmitted jobs, script errors or waiting for dependencies jobs')
		self.last_update = time.time()

	def get_status_string(self,message='Queue updated'):
		return time.strftime("[%Y %m %d %H:%M:%S]: "+message+"\n"+str(self), time.localtime())

	def save_status(self,message='Queue updated'):
		if not os.path.isdir(self.path):#'jobs'):
			os.makedirs(self.path)#'jobs')
		with open(os.path.join(self.path,self.name+'.jq_status'),'a') as f_status:#'jobs/'+self.name+'.jq_status','a') as f_status:
			f_status.write(self.get_status_string(message=message))

		try:
			os.symlink(os.path.join(self.jobqueue_dir,self.name+'.jq_status'),os.path.join(self.original_path,self.name+'.jq_status'))
		except OSError as e:
			if e.errno == errno.EEXIST:
				os.remove(os.path.join(self.original_path,self.name+'.jq_status'))
				os.symlink(os.path.join(self.jobqueue_dir,self.name+'.jq_status'),os.path.join(self.original_path,self.name+'.jq_status'))

	def __str__(self):
		total = 0
		ans = {}
		for j in self.job_list:
			total +=1
			if not j.status in list(ans.keys()):
				ans[j.status] = 1
			else:
				ans[j.status] += 1
		exec_time = self.past_exec_time
		exec_time_j = int(exec_time/86400)
		exec_time -= exec_time_j * 86400
		exec_time_h = int(exec_time/3600)
		exec_time -= exec_time_h * 3600
		exec_time_m = int(exec_time/60)
		exec_time -= exec_time_m * 60
		str_exec = ''
		if exec_time_j:
			str_exec += str(int(exec_time_j)) + ' days '
		if exec_time_h:
			str_exec += str(int(exec_time_h))+' h '
		if exec_time_m:
			str_exec += str(int(exec_time_m))+' min '
		str_exec +=str(int(exec_time))+' s'
		str_ans = '    total: '+str(total)+'\n    '+'\n    '.join([str(key)+': '+str(val) for key,val in list(ans.items())])
		if not hasattr(self,'restarted_jobs'):
			self.restarted_jobs = 0
		if not hasattr(self,'extended_jobs'):
			self.extended_jobs = 0
		str_ans += '\n\n    execution time: '+str_exec+'\n    jobs done: '+str(self.executed_jobs)+'\n    jobs restarted: '+str(self.restarted_jobs)+'\n    jobs extended: '+str(self.extended_jobs)+'\n'
		return str_ans

	def auto_finish_queue(self,t=10,coeff=1,call_between=None,clear_output=True):
		self.update_queue()
		step = t
		state = str(self)
		while [j for j in self.job_list if (j.status != 'missubmitted' and j.status != 'dependencies not satisfied')]:
			time.sleep(step)
			self.update_queue(clear_output=clear_output)
			if str(self) == state:
				step *= coeff
			else:
				state = str(self)
				step = t
			if call_between is not None:
				call_between()
			if self.job_list and not [j for j in self.job_list if j.status != 'to be cleaned']:
				self.update_queue(clear_output=clear_output)
		self.clean_jobqueue()

	def check_virtualenvs(self):
		envs = {}
		for j in self.job_list:
			env = str(j.virtual_env)
			if env not in list(envs.keys()):
				envs[env] = copy.deepcopy(j.requirements)
			else:
				envs[env] += copy.deepcopy(j.requirements)
		for env in list(envs.keys()):
			if not hasattr(self,'requirements'):
				self.requirements = []
			if env == 'None':
				self.check_python_version()
				self.update_virtualenv(None, requirements=list(set(envs[env]+self.requirements)))
			else:
				self.check_python_version(virtual_env=env)
				self.update_virtualenv(env, requirements=list(set(envs[env]+self.requirements)))

	def cancel_job(self, job, clean=False):
		if self.erase:
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

	def reinit_missubmitted(self,job=None, force=False):
		if job is None:
			jlist = self.job_list
		else:
			jlist = [job]
		for j in jlist:
			if j.status == 'missubmitted':
				if force or ( hasattr(j,'reinit_missubmitted_times') and j.reinit_missubmitted_times >0):
					#j.update()
					j.restart()
					if hasattr(j,'reinit_missubmitted_times'):
						j.reinit_missubmitted_times -= 1
					j.status = 'pending'
					if hasattr(self,'restarted_jobs'):
						self.restarted_jobs += 1
					else:
						self.restarted_jobs = 1

	def submit_job(self, job):
		pass

	def global_submit(self):
		pass

	def check_running_jobs(self):
		pass

	def set_virtualenv(self, virtual_env, requirements):
		pass

	def update_virtualenv(self, virtual_env, requirements):
		pass

	def retrieve_job(self, job):
		pass

	def clean_jobqueue(self):
		pass

	def global_retrieval(self):
		return []

	def avail_workers(self):
		return 1

	def get_errors(self):
		errors = []
		for j in self.job_list:
			if j.status in ['missubmitted','script error']:
				errors.append((j.job_dir,j.get_error()))
		return errors

	def print_errors(self,n=None):
		errors = self.get_errors()
		if n is None:
			nmax = len(errors)
		else:
			nmax = n
		for a,b in errors:
			print(b)
			print("==============")


	def check_python_version(self,virtual_env=None):
		pass