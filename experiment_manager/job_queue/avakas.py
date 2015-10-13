
import os
import shutil
import time

from . import BaseJobQueue
from ...additional.ssh import SSHSession

class AvakasJobQueue(BaseJobQueue)
	def __init__(self, ssh_cfg, basedir='HOME/jobs'):
		super(self,AvakasJobQueue).__init__()
		if basedir[:4] == 'HOME':
			self.basedir = '/home/' + self.ssh_cfg.username + basedir[4:]
		else:
			self.basedir = basedir
		self.ssh_cfg = ssh_cfg


	def format_dict(self, job):

		if job.virtual_env is None:
			python_dir = '/usr'
		else:
			python_dir = '/home/{}/virtualenvs/{}'.format(self.ssh_cfg['username'], job.virtual_env)

		job_name = '_'.join(time.strftime('%Y-%m-%d_%H-%M-%S'), job.descr, job.uuid)

		local_job_dir = './.jobs/' + job_name

		format_dict = {
			'username':self.ssh_cfg['username'],
			'basedir': self.basedir,
			'base_work_dir': '/tmp/' # '/scratch/'+self.ssh_cfg['username']+'/'
			'virtual_env': job.virtual_env,
			'python_dir':python_dir,
			'job_name': job_name,
			'job_dir': self.basedir + '/' + job_name
			'local_job_dir': local_job_dir
			'job_descr': job.descr,
			'job_uuid': job.uuid,
			'walltime': job.estimated_time
		}



	def submit_job(self, job):
		if not job.status == 'pending':
			print('Job {} already submitted'.format(job.uuid))
			pass
		job.status = 'missubmitted'
		format_dict = self.format_dict(job)
		session = SSHSession(**ssh_cfg)
		os.makedirs(local_job_dir)
		with open(local_job_dir+ "/pbs.py", "w") as pbs_file:
			pbs_file.write("""
#! {4}/bin/python
#PBS -o {0}/{1}/output.txt
#PBS -e {0}/{1}/error.txt
#PBS -l walltime={2}
#PBS -l nodes=1:ppn=1
#PBS -N {3}


import os
import shutil
import cPickle

os.environ['PBS_JOBID'] = PBS_JOBID
job_dir = {job_dir}
work_dir = '{base_work_dir}'+PBS_JOBID

shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.b','r') as f:
	job = cPickle.loads(f.read())

job.run()

""".format(**format_dict))


		with open(local_job_dir + "/epilogue.sh", "w") as pbs_file:
			pbs_file.write("""
#!/bin/bash
cp -f {base_work_dir}/$PBS_JOBID/* {job_dir}/
""".format(**format_dict))



		job.make_files(directory=format_dict['local_job_dir'])

		session.command('module load torque')
		session.create_path("{job_dir}".format(**format_dict))
		for up_file in ['pbs.py', 'epilogue.sh', 'job.b', data_file]
			session.put(
				"{local_job_dir}/".format(**format_dict) + up_file,
				"{job_dir}/".format(**format_dict) + up_file)

		session.command("qsub -l epilogue={job_dir}/epilogue.sh {job_dir}/pbs.py".format(directory))
		session.close()

		job.status = 'running'
		time.sleep(1)


	def check_job(self, job):
		CONNECT, QSTAT JOB, IF NOT RUNNING RETRIEVE
		IF NOT JOB RUNNING:
		job.status = 'unfinished'

	def set_virtualenv(self, virtual_env, requirements):
		session = SSHSession(**ssh_cfg)
		if virtual_env is None:
			for package in requirements:
				session.command('pip install --user '+package)
		else:
			if not session.path_exists('$HOME/virtualenvs/'+virtual_env):
				session.command('virtualenv $HOME/virtualenvs/'+virtual_env)
			session.command('source $HOME/virtualenvs/'+virtual_env+'/activate')
			for package in requirements:
				session.command('pip install '+package)
			session.command(deactivate)
		session.close()

	def update_virtualenv(self, virtual_env, requirements=[]):
		session = SSHSession(**ssh_cfg)
		if not session.path_exists('$HOME/virtualenvs/'+virtual_env):
			session.close()
			self.set_virtualenv(virtual_env=virtual_env, requirements=requirements)
		else:
			if virtual_env is not None:
				session.command('source $HOME/virtualenvs/'+virtual_env+'/activate')
				option=''
			else:
				option='--user '
			for package in requirements:
				if package is None:
					session.command("pip freeze --local | grep -v '^\-e' | cut -d = -f 1  | xargs pip install -U")
				else:
					session.command('pip install --upgrade '+option+package)
				if virtual_env is not None:
					session.command(deactivate)
			session.close()

	def retrieve_data(self, job):
		pass
