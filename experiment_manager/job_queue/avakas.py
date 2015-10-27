
import os
import shutil
import time
import copy

from . import JobQueue
from ..tools.ssh import SSHSession

class AvakasJobQueue(JobQueue):
	def __init__(self, ssh_cfg, basedir='jobs', max_jobs=100, **kwargs):
		super(AvakasJobQueue,self).__init__(**kwargs)
		self.max_jobs = max_jobs
		self.ssh_cfg = ssh_cfg
		self.basedir = basedir
		if basedir[0] == '/':
			raise IOError('basedir must be relative path')
		if 'hostname' not in self.ssh_cfg.keys():
			self.ssh_cfg['hostname'] = 'avakas.mcia.univ-bordeaux.fr'
		if 'key_file' not in self.ssh_cfg.keys() and 'password' not in self.ssh_cfg.keys():
			self.ssh_cfg['key_file'] = 'avakas'


	def format_dict(self, job):

		if job.virtual_env is None:
			python_bin = '/usr/bin/env python'
		else:
			python_bin = '/home/{}/virtualenvs/{}/bin/python'.format(self.ssh_cfg['username'], job.virtual_env)

		time = job.estimated_time

		walltime_h = int(time/3600)
		time -= 3600*walltime_h
		if walltime_h<10:
			walltime_h = '0'+str(walltime_h)
		else:
			walltime_h = str(walltime_h)

		walltime_m = int(time/60)
		time -= 60*walltime_m
		if walltime_m<10:
			walltime_m = '0'+str(walltime_m)
		else:
			walltime_m = str(walltime_m)

		walltime_s = time
		if walltime_s<10:
			walltime_s = '0'+str(walltime_s)
		else:
			walltime_s = str(walltime_s)

		format_dict = {
			'username':self.ssh_cfg['username'],
			'basedir': self.basedir,
			'base_work_dir': '/tmp/', # '/scratch/'+self.ssh_cfg['username']+'/'
			'virtual_env': job.virtual_env,
			'python_bin': python_bin,
			'job_name': job.job_dir,
			'job_dir': os.path.join(self.basedir,job.job_dir),
			'local_job_dir': job.path,
			'job_descr': job.descr,
			'job_uuid': job.uuid,
			'walltime': ':'.join([walltime_h, walltime_m, walltime_s])
		}

		return format_dict


	def submit_job(self, job):
		if not job.status == 'pending':
			print('Job {} already submitted'.format(job.uuid))
			pass
		job.status = 'missubmitted'
		format_dict = self.format_dict(job)
		session = SSHSession(**self.ssh_cfg)
		if not os.path.exists(format_dict['local_job_dir']):
			os.makedirs(format_dict['local_job_dir'])
		with open("{local_job_dir}/pbs.py".format(**format_dict), "w") as pbs_file:
			pbs_file.write(
"""#!{python_bin}
#PBS -o {job_dir}/output.txt
#PBS -e {job_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {job_name}


import os
import sys
import shutil
import jsonpickle

PBS_JOBID = os.environ['PBS_JOBID']
job_dir = '{job_dir}'
work_dir = '{base_work_dir}'+PBS_JOBID

shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.b','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict))


		with open("{local_job_dir}/epilogue.sh".format(**format_dict), "w") as epilogue_file:
			epilogue_file.write(
"""#!/bin/bash
echo "job finished, backing up files."
PBS_JOBID=$1
cp -f -R {base_work_dir}$PBS_JOBID/* {job_dir}/
exit 0
""".format(**format_dict))


		session.command('module load torque')
		session.create_path("{job_dir}".format(**format_dict))
		session.put_dir(format_dict['local_job_dir'], format_dict['job_dir'])
		session.command_output('chmod u+x {job_dir}/epilogue.sh'.format(**format_dict))
		session.command_output('chmod u+x {job_dir}/pbs.py'.format(**format_dict))
		job.PBS_JOBID = session.command_output("qsub -l epilogue={job_dir}/epilogue.sh {job_dir}/pbs.py".format(**format_dict))
		session.close()

		job.status = 'running'
		time.sleep(1)


	def check_job_running(self, job):
		session = SSHSession(**self.ssh_cfg)
		test = session.command_output('qstat -f|grep {job_name}'.format(**self.format_dict(job)))
		session.close()
		if test:
			return True
		else:
			return False

	def retrieve_job(self, job):
		path = copy.deepcopy(job.path)
		session = SSHSession(**self.ssh_cfg)
		job_dir = self.format_dict(job)['job_dir']
		local_job_dir = self.format_dict(job)['local_job_dir']
		session.get_dir(job_dir, local_job_dir)
		session.close()
		job.update()
		job.path = path

	def set_virtualenv(self, virtual_env, requirements, sys_site_packages=True):
		session = SSHSession(**self.ssh_cfg)
		cmd = []
		if sys_site_packages:
			site_pack = '--system-site-packages '
		else:
			site_pack = ''
		if not isinstance(requirements, (list, tuple)):
			requirements = [requirements]
		if virtual_env is None:
			for package in requirements:
				session.command('pip install --user '+package)
		else:
			if not session.path_exists('/home/{}/virtualenvs/{}'.format(self.ssh_cfg['username'], virtual_env)):
				cmd.append('virtualenv {}/home/{}/virtualenvs/{}'.format(site_pack,self.ssh_cfg['username'], virtual_env))
			cmd.append('source /home/{}/virtualenvs/{}/bin/activate'.format(self.ssh_cfg['username'], virtual_env))
			for package in requirements:
				cmd.append('pip install '+package)
			cmd.append('deactivate')
			print session.command_output(' && '.join(cmd))
		session.close()

	def update_virtualenv(self, virtual_env=None, requirements=[]):
		cmd = []
		if not isinstance(requirements, (list, tuple)):
			requirements = [requirements]
		session = SSHSession(**self.ssh_cfg)
		if not session.path_exists('/home/{}/virtualenvs/{}'.format(self.ssh_cfg['username'], virtual_env)):
			session.close()
			self.set_virtualenv(virtual_env=virtual_env, requirements=requirements)
		else:
			if virtual_env is not None:
				cmd.append('source /home/{}/virtualenvs/{}/bin/activate'.format(self.ssh_cfg['username'], virtual_env))
				option=''
			else:
				option='--user '
			if requirements == ['all']:
					cmd.append("pip freeze --local | grep -v '^\-e' | cut -d = -f 1  | xargs pip install -U "+option)
			else:
					cmd.append('pip install --upgrade --no-deps '+option+' '.join(requirements))
					cmd.append('pip install '+option+' '.join(requirements))
			if virtual_env is not None:
				cmd.append('deactivate')
			print session.command_output(' && '.join(cmd))
			session.close()

	def cancel_job(self, job, clean=False):
		if job.status == 'running':
			session = SSHSession(**self.ssh_cfg)
			session.command_output('qdel ' + job.PBS_JOBID)
			session.close()
		super(AvakasJobQueue, self).cancel_job(job, clean=clean)

	def avail_workers(self):
		session = SSHSession(**self.ssh_cfg)
		qstat = session.command_output('qstat |grep ' + self.ssh_cfg['username'])
		return self.max_jobs - len(qstat.split('\n')) + 1
