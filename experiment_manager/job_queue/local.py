
from . import JobQueue
import pip
import os
import glob

import multiprocessing as mp

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





class LocalMultiProcessJobQueue(LocalJobQueue):
	def __init__(self, nb_process=None, **kwargs):
		LocalJobQueue.__init__(self,**kwargs)
		if nb_process is None:
			self.nb_process = mp.cpu_count()
		else:
			self.nb_process = nb_process
		self.pool = mp.Pool(processes=self.nb_process)
		#backupdir?

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		del out_dict['pool']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		self.pool = mp.Pool(processes=self.nb_process)


	def submit_job(self, job):
		pass

	def check_job(self, job):
		if job.status == 'running':
			job.status = 'unfinished'

	def global_submit(self):
		waiting_list = [jj for jj in self.job_list if jj.status == 'pending']
		##self.pool.add(waiting_list.run)

