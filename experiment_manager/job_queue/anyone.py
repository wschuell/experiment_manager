
from slurm import OldSlurmJobQueue

class AnyoneJobQueue(OldSlurmJobQueue):
	def __init__(self, username=None,hostname='anyone', basedir=None, local_basedir=None, base_work_dir=None, max_jobs=256, key_file='cluster_roma', password=None, **kwargs):
		if username is None:
			username = self.get_username_from_hostname(hostname)
		ssh_cfg = {'username':username,
					'hostname':hostname}
		if not self.check_hostname(hostname):
			print 'Hostname '+hostname+' not in your .ssh/config'
			ssh_cfg = {'username':username,
					'hostname':'anyone.phys.uniroma1.it',
					'key_file':key_file,
					'password':password
					}
		if basedir is None:
			basedir = '/home/'+username
		if base_work_dir is None:
			base_work_dir = '/home/'+username+'/work_dir' #'/data'
		if local_basedir is None:
			local_basedir = ''
		OldSlurmJobQueue.__init__(self,ssh_cfg=ssh_cfg,base_work_dir=base_work_dir,basedir=basedir,local_basedir=local_basedir, max_jobs=max_jobs, install_as_job=False, **kwargs)
