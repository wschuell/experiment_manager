
from . import JobQueue
import pip
import os
import glob
import sys

import multiprocessing as mp

from ..job import run_job_from_path

class LocalJobQueue(JobQueue):
	def __init__(self, **kwargs):
		super(LocalJobQueue, self).__init__(**kwargs)
		self.backupdir = 'backup_dir'

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

	def check_backups(self):
		self.backups_status = {'present':[],'locked':[]}
		presentbackups = [os.path.basename(elt) for elt in glob.glob(os.path.join(self.backupdir,'*'))]
		lockedbackups = [os.path.basename(elt) for elt in glob.glob(os.path.join(self.backupdir,'backup_lock','*'))]
		for j in self.job_list:
			if j.uuid in presentbackups:
				self.backups_status['present'].append(j.uuid)
				if j.uuid in lockedbackups:
					self.backups_status['locked'].append(j.uuid)

	def retrieve_job(self, job):
		if job.uuid in self.backups_status['present'] and not job.uuid in self.backups_status['locked']:
			shutil.copytree(os.path.join(self.backupdir,job.uuid),job.path)
		JobQueue.retrieve_job(self,job)


class JobProcess(mp.Process):
	def __init__(self, job_path, *vargs, **kwargs):
		self.job_path = job_path
		mp.Process.__init__(self,target=run_job_from_path,args=(self.job_path,), *vargs, **kwargs)

	def run(self):
		self.init_redirect()
		mp.Process.run(self)

	def init_redirect(self):
		sys.stdout = open(os.path.join(self.job_path,"output.txt"), "a", buffering=0)
		sys.stderr = open(os.path.join(self.job_path,"error.txt"), "a", buffering=0)


class LocalMultiProcessJobQueue(LocalJobQueue):
	def __init__(self, nb_process=None, **kwargs):
		LocalJobQueue.__init__(self,**kwargs)
		if nb_process is None:
			self.nb_process = mp.cpu_count()
		else:
			self.nb_process = nb_process
		self.running_processes = []
		#self.pool = mp.Pool(processes=self.nb_process)
		#backupdir?

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		del out_dict['running_processes']
		#del out_dict['pool']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		self.running_processes = []
		#self.pool = mp.Pool(processes=self.nb_process)


	def submit_job(self, job):
		#p = mp.Process(target=run_job_from_path,args=(job.path,))
		p = JobProcess(job_path=job.path)
		self.running_processes.append((p,job.uuid))
		job.status = 'running'
		p.start()


	def global_submit(self):
		pass


	def check_running_jobs(self):
		self.finished_running_jobs = []
		still_running_uuids = []
		for p,j_uuid in list(self.running_processes):
			if not p.is_alive():
				p.join()
				self.running_processes.remove((p,j_uuid))
			else:
				still_running_uuids.append(j_uuid)
		for j in self.job_list:
			if j.status == 'running' and j.uuid not in still_running_uuids:
				j.status = 'finished running'

	def avail_workers(self):
		self.refresh_avail_workers()
		if hasattr(self,'waiting_to_submit'):
			offset_waiting = len(self.waiting_to_submit)
		else:
			offset_waiting = 0
		return self.available_workers - offset_waiting

	def refresh_avail_workers(self):
		Njobs = self.count_running_jobs()
		self.available_workers = self.nb_process - Njobs

	def count_running_jobs(self):
		return len(self.running_processes)

	def retrieve_job(self,job):
		job.update()

