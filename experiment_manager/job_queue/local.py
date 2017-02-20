
from . import JobQueue
import pip
import glob

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
