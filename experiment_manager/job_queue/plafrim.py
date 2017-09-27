
from slurm import SlurmJobQueue,OldSlurmJobQueue

class PlafrimJobQueue(SlurmJobQueue):
	def __init__(self, username=None,hostname='plafrim-ext', basedir=None, local_basedir='', base_work_dir=None, max_jobs=256, key_file='plafrim', password=None, install_as_job=False, modules = [], **kwargs):
		if username is None:
			username = self.get_username_from_hostname(hostname)
		ssh_cfg = {'username':username,
					'hostname':hostname,
					}
		if not self.check_hostname(hostname):
			raise ValueError('Hostname '+hostname+' not in your .ssh/config\n')
		if basedir is None:
			basedir = '/lustre/'+username
		if base_work_dir is None:
			base_work_dir = '/tmp/'+username
		if not [_ for _ in modules if 'slurm' in _]:
			modules.append('slurm')
		SlurmJobQueue.__init__(self,ssh_cfg=ssh_cfg,modules=modules,base_work_dir=base_work_dir,basedir=basedir,local_basedir=local_basedir, max_jobs=max_jobs, install_as_job=install_as_job, **kwargs)
