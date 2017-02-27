
from slurm import SlurmJobQueue

class AnyoneJobQueue(SlurmJobQueue):
	def __init__(self, username, basedir=None, local_basedir=None, base_work_dir='/data', max_jobs=256, key_file='cluster_roma', password=None, **kwargs):
		ssh_cfg = {'username':username,
					'hostname':'anyone.phys.uniroma1.it',
					'key_file':key_file,
					'password':password
					}
		if basedir is None:
			basedir = '/home/'+username+'/jobs'
		if local_basedir is None:
			local_basedir = 'jobs'
		SlurmJobQueue.__init__(self,ssh_cfg=ssh_cfg,basedir=basedir,local_basedir=local_basedir, max_jobs=max_jobs, **kwargs)
