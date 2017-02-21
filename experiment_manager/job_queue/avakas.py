
import os
import shutil
import time
import copy
import uuid

from . import JobQueue
from ..tools.ssh import SSHSession

class AvakasJobQueue(JobQueue):
	def __init__(self,username=None, ssh_cfg={}, basedir=None, max_jobs=1000, **kwargs):
		super(AvakasJobQueue,self).__init__(**kwargs)
		self.max_jobs = max_jobs
		self.ssh_cfg = ssh_cfg
		self.update_needed = False
		if basedir is not None and basedir[0] == '/':
			raise IOError('basedir must be relative path')
		if username is not None:
			self.ssh_cfg['username'] = username
		if 'hostname' not in self.ssh_cfg.keys():
			self.ssh_cfg['hostname'] = 'avakas.mcia.univ-bordeaux.fr'
		if 'key_file' not in self.ssh_cfg.keys() and 'password' not in self.ssh_cfg.keys():
			self.ssh_cfg['key_file'] = 'avakas'
		self.ssh_session = SSHSession(**self.ssh_cfg)
		if basedir is None:
			self.basedir = '/scratch/'+self.ssh_cfg['username']+'/jobs'
		else:
			self.basedir = basedir
		self.waiting_to_submit = {}
		self.remote_backupdir = os.path.join(self.basedir,'backup_dir')


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

		if hasattr(job,'PBS_JOBID'):
			pbs_jobid = job.PBS_JOBID
		else:
			pbs_jobid = 'NO_PBS_JOBID'

		format_dict = {
			'username':self.ssh_cfg['username'],
			'basedir': self.basedir,
			'base_work_dir': '/tmp/', # '/scratch/'+self.ssh_cfg['username']+'/'
			'virtual_env': job.virtual_env,
			'python_bin': python_bin,
			'job_name': job.job_dir,
			'job_dir': os.path.join(self.basedir,job.job_dir),
			'job_backup_dir': os.path.join(self.basedir,job.backup_dir,job.uuid),
			'local_job_dir': job.path,
			'job_descr': job.descr,
			'job_uuid': job.uuid,
			'job_pbsjobid': pbs_jobid,
			'walltime': ':'.join([walltime_h, walltime_m, walltime_s])
		}

		return format_dict


	def individual_submit_job(self, job):
		if not job.status == 'pending':
			print('Job {} already submitted'.format(job.uuid))
		job.status = 'missubmitted'
		job.backup_dir = 'backup_dir'
		format_dict = self.format_dict(job)
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
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

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict))


		with open("{local_job_dir}/epilogue.sh".format(**format_dict), "w") as epilogue_file:
			epilogue_file.write(
"""#!/bin/bash
echo "Job finished, backing up files."
PBS_JOBID=$1
cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir
cp -f -R {base_work_dir}\"$PBS_JOBID\"/* {job_dir}/
rm -R {base_work_dir}$PBS_JOBID
echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict))


		session.command('module load torque')
		session.create_path("{job_dir}".format(**format_dict))
		for f in ['pbs.py','epilogue.sh']:
			if f not in job.files:
				job.files.append(f)
		for f in job.files:
			session.put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
			session.batch_put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
		session.batch_send()
		session.command_output('chmod u+x {job_dir}/epilogue.sh'.format(**format_dict))
		session.command_output('chmod u+x {job_dir}/pbs.py'.format(**format_dict))
		job.PBS_JOBID = session.command_output("qsub -l epilogue={job_dir}/epilogue.sh {job_dir}/pbs.py".format(**format_dict))[:-1]
		#session.close()
		if job.PBS_JOBID:
			job.status = 'running'
		else:
			raise Exception('No PBSJOBID returned on qsub command')
		job.save()
		#time.sleep(0.2)

	def submit_job(self, job):
		if not job.status == 'pending':
			print('Job {} already submitted'.format(job.uuid))
		job.status = 'missubmitted'
		job.backup_dir = 'backup_dir'
		format_dict = self.format_dict(job)
		wt = format_dict['walltime']
		if wt not in self.waiting_to_submit.keys():
			self.waiting_to_submit[wt] = []
		self.waiting_to_submit[wt].append(job)#consider walltime

	def global_submit(self):
		session = self.ssh_session
		jobdir_dict = {}
		for wt,j_list in self.waiting_to_submit.items():
			jobdir_dict[wt] = {}
			for i in range(len(j_list)):
				jobdir_dict[wt][i+1] = self.format_dict(j_list[i])['job_dir']

		for wt,j_list in self.waiting_to_submit.items():
			format_dict = self.format_dict(j_list[0])
			multijob_dir = self.name+'_'+self.uuid+'_'+j_list[0].uuid+'_'+time.strftime("%Y%m%d%H%M%S", time.localtime())
			format_dict .update({
				'jobdir_dict':str(jobdir_dict[wt]),
				'multijob_dir':os.path.join(self.basedir,multijob_dir),
				'local_multijob_dir':os.path.join(self.path,multijob_dir),
				'multijob_name':multijob_dir,
				'Njobs':len(j_list)
			})

			if not os.path.exists(format_dict['local_multijob_dir']):
				os.makedirs(format_dict['local_multijob_dir'])
			session.create_path("{multijob_dir}".format(**format_dict))
			session.command('module load torque')
			with open("{local_multijob_dir}/pbs.py".format(**format_dict), "w") as pbs_file:
				pbs_file.write(
"""#!{python_bin}
#PBS -o {multijob_dir}/output.txt
#PBS -e {multijob_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {multijob_name}


import os
import sys
import shutil
import jsonpickle

PBS_JOBID = os.environ['PBS_JOBID']
PBS_ARRAYID = os.environ['PBS_ARRAYID']

jobdir_dict = {jobdir_dict}

job_dir = jobdir_dict[int(PBS_ARRAYID)]
work_dir = '{base_work_dir}'+PBS_JOBID


shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict))


			with open("{local_multijob_dir}/epilogue.sh".format(**format_dict), "w") as epilogue_file:
				epilogue_file.write(
"""#!/bin/bash
echo "Job finished, backing up files."
PBS_JOBID=$1

MULTIJOBDIR={multijob_dir}
ARRAYID=$(python -c "jobid='"$PBS_JOBID"'; print jobid.split('[')[1].split(']')[0]")
JOBDIR=$(python -c "jobdir_dict = {jobdir_dict}; print jobdir_dict["$ARRAYID"]")

cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir

cp -f -R {base_work_dir}\"$PBS_JOBID\"/* $JOBDIR/
rm -R {base_work_dir}$PBS_JOBID



echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict))

			for job in j_list:
				format_dict_job = self.format_dict(job)
				job.multijob_dir = format_dict['multijob_dir']
				if not os.path.exists(format_dict_job['local_job_dir']):
					os.makedirs(format_dict_job['local_job_dir'])
				session.create_path("{job_dir}".format(**format_dict_job))
				for f in job.files:
					#session.put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))
					session.batch_put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))

			for f in ['pbs.py','epilogue.sh']:
				session.put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
				session.batch_put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
			session.batch_send()
			session.command_output('chmod u+x {multijob_dir}/epilogue.sh'.format(**format_dict))
			session.command_output('chmod u+x {multijob_dir}/pbs.py'.format(**format_dict))

			PBS_JOBID = session.command_output("qsub -t 1-{Njobs} -l epilogue={multijob_dir}/epilogue.sh {multijob_dir}/pbs.py".format(**format_dict))[:-1]
			#session.close()
			if PBS_JOBID:
				for i in range(len(j_list)):
					job = j_list[i]
					job.PBS_JOBID = PBS_JOBID.split('[')[0] + '[' + str(i+1) + ']' + PBS_JOBID.split(']')[1]
					job.status = 'running'
					job.array_id = i+1
					job.save()
			else:
				raise Exception('No PBSJOBID returned by qsub, multijob '+str(format_dict['multijob_dir']))
				#for i in range(len(j_list)):
				#	job = j_list[i]
				#	job.save()
		self.waiting_to_submit = {}

	def check_running_jobs(self):
		self.finished_running_jobs = []
		session = self.ssh_session
		running_jobs_string = session.command_output('qstat -f -t|grep \'Job Id:\'')
		for j in self.job_list:
			if j.status == 'running' and running_jobs_string.find(j.PBS_JOBID) == -1:
				j.status = 'finished running'
		self.refresh_avail_workers()

	def check_backups(self):
		self.backups_status = {'present':[],'locked':[]}
		session = self.ssh_session
		presentbackups = session.command_output('ls -l '+self.remote_backupdir)
		lockedbackups = session.command_output('ls -l '+os.path.join(self.remote_backupdir,'backup_lock'))
		for j in self.job_list:
			if j.uuid in presentbackups:
				self.backups_status['present'].append(j.uuid)
				if j.uuid in lockedbackups:
					self.backups_status['locked'].append(j.uuid)

	def retrieve_job(self, job):
		path = copy.deepcopy(job.path)
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		if job.uuid in self.backups_status['present'] and not job.uuid in self.backups_status['locked']:
			job_dir = self.format_dict(job)['job_backup_dir']
		else:
			job_dir = self.format_dict(job)['job_dir']
		local_job_dir = self.format_dict(job)['local_job_dir']
		if hasattr(job,'multijob_dir'):
			for f in ['output.txt','error.txt']:
				session.command_output('cp {multijob_dir}/{f}-{array_id} {dir}/{f}'.format(f=f,dir=job_dir,multijob_dir=job.multijob_dir,array_id=job.array_id,**self.format_dict(job)))
		if hasattr(job,'clean_at_retrieval'):
			for f in job.clean_at_retrieval:
				session.remove(os.path.join(job_dir,f))
		#session.batch_get(job_dir, local_job_dir)
		#session.batch_receive()
		session.get_dir(job_dir, local_job_dir)

		#session.close()
		job.update()
		job.path = path

	def set_virtualenv(self, virtual_env, requirements, sys_site_packages=True):
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
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
			#out = session.command_output(' && '.join(cmd))
			out = self.command_output_qsub(' && '.join(cmd),retry=True)
		#session.close()

	def update_virtualenv(self, virtual_env=None, requirements=[],src_path=None):
		cmd = []
		if src_path is None:
			src_path = '/scratch/'+self.ssh_cfg['username']+'/src_'+self.uuid
		if not isinstance(requirements, (list, tuple)):
			requirements = [requirements]
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		if virtual_env is not None and not session.path_exists('/home/{}/virtualenvs/{}'.format(self.ssh_cfg['username'], virtual_env)):
			#session.close()
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
					cmd.append('pip install --upgrade --no-deps --src '+src_path+' '+option+' '.join(requirements))
					cmd.append('pip install --src '+src_path+' '+option+' '.join(requirements))
			if virtual_env is not None:
				cmd.append('deactivate')
			#out = session.command_output(' && '.join(cmd))
			out = self.command_output_qsub(' && '.join(cmd),retry=True)
			#session.close()

	def command_output_qsub(self,cmd,t_min=10,retry=False,retry_time=30):
		cmd_uuid = str(uuid.uuid1())
		cmd_path = os.path.join(self.basedir,'tempcommand_'+cmd_uuid)
		out = self.ssh_session.command_output('mkdir -p '+cmd_path)
		file_path = os.path.join(cmd_path,'cmd.sh')
		output_path = os.path.join(cmd_path,'output.txt')
		self.ssh_session.command_output('echo \"'+cmd+'\" > '+file_path)
		cmdjob_id = self.ssh_session.command_output('qsub -l walltime=00:'+str(t_min)+':00 -l nodes=1:ppn=1 -j oe -o '+output_path+' '+file_path)
		t = time.time()
		while not self.ssh_session.path_exists(output_path):
			if time.time()-t > 60*t_min:
				if retry:
					return self.command_output_qsub(cmd,t_min=retry_time,retry=False)
				else:
					raise Exception('Command is taking too long, might be blocked')
			time.sleep(5)
		return self.ssh_session.command_output('cat '+output_path)
		#self.ssh_session.command_output('rm -R '+cmd_path)


	def cancel_job(self, job, clean=False):
		if job.status == 'running':
			#session = SSHSession(**self.ssh_cfg)
			session = self.ssh_session
			session.command_output('qdel ' + job.PBS_JOBID)
			#session.close()
		super(AvakasJobQueue, self).cancel_job(job, clean=clean)

	def avail_workers(self):
		#session = SSHSession(**self.ssh_cfg)
		if not hasattr(self,'available_workers'):
			self.refresh_avail_workers()
		offset_waiting = sum([len(j_list) for j_list in self.waiting_to_submit.values()])
		return self.available_workers - offset_waiting
		#session.close()

	def refresh_avail_workers(self):
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		qstat = int(session.command_output('qstat -u {} -t|wc -l'.format(self.ssh_cfg['username'])))
		if qstat > 0:
			qstat -= 5
		self.available_workers = self.max_jobs - qstat
		#session.close()

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		del out_dict['ssh_session']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		self.ssh_session = SSHSession(**self.ssh_cfg)
