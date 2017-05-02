
import os
import shutil
import time
import copy
import uuid
import stat

from . import JobQueue
from ..tools.ssh import SSHSession

class ClusterJobQueue(JobQueue):
	def __init__(self, ssh_cfg={}, basedir='', local_basedir='', max_jobs=1000, base_work_dir=None, without_epilogue=False, **kwargs):
		super(ClusterJobQueue,self).__init__(**kwargs)
		self.max_jobs = max_jobs
		self.ssh_cfg = ssh_cfg
		self.update_needed = False
		self.ssh_session = SSHSession(**self.ssh_cfg)
		self.waiting_to_submit = {}
		self.basedir = basedir
		self.local_basedir = local_basedir
		self.remote_backupdir = os.path.join(self.basedir,'backup_dir')
		self.without_epilogue = without_epilogue
		if base_work_dir is None:
			self.base_work_dir = self.basedir
		else:
			self.base_work_dir = base_work_dir
		if len(self.base_work_dir)>1 and self.base_work_dir[-1] == '/':
			self.base_work_dir = self.base_work_dir[:-1]

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

		if hasattr(job,'JOBID'):
			jobid = job.JOBID
		else:
			jobid = 'NO_JOBID'

		format_dict = {
			'username':self.ssh_cfg['username'],
			'basedir': self.basedir,
			'base_work_dir': self.base_work_dir,
			'virtual_env': job.virtual_env,
			'python_bin': python_bin,
			'job_name': job.job_dir,
			'job_dir': os.path.join(self.basedir,job.job_dir),
			'job_backup_dir': os.path.join(self.basedir,job.backup_dir,job.uuid),
			'local_job_dir': job.path,
			'job_descr': job.descr,
			'job_uuid': job.uuid,
			'job_jobid': jobid,
			'walltime_seconds': job.estimated_time,
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

		special_files = self.gen_files(format_dict=format_dict)
		for filename,content in special_files:
			with open("{local_job_dir}/{special_filename}".format(special_filename=filename,**format_dict), "w") as special_file:
				special_file.write(content)

		#with open("{local_job_dir}/script.py".format(**format_dict), "w") as script_file:
		#	script_file.write(self.individual_script(format_dict=format_dict))


		#with open("{local_job_dir}/epilogue.sh".format(**format_dict), "w") as epilogue_file:
		#	epilogue_file.write(self.individual_epilogue(format_dict=format_dict))


		#session.create_path("{job_dir}".format(**format_dict))
		for f,c in special_files:
			st = os.stat(os.path.join(format_dict['local_job_dir'],f))
			os.chmod(os.path.join(format_dict['local_job_dir'],f), st.st_mode | stat.S_IXUSR)
			if f not in job.files:
				job.files.append(f)
		for f in job.files:
			session.put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
			session.batch_put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
		session.batch_send(untar_basedir=self.basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=self.command_asjob_output)
		#session.command_output('chmod u+x {job_dir}/epilogue.sh'.format(**format_dict))
		#session.command_output('chmod u+x {job_dir}/script.py'.format(**format_dict))
		JOBID = self.send_submit_command(cmd_type='single_job',format_dict=format_dict)
		job.JOBID = self.jobid_from_submit_output(JOBID)
		#session.close()
		if job.JOBID:
			job.status = 'running'
		else:
			raise Exception('No JOBID returned on submit command')
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
			#session.create_path("{multijob_dir}".format(**format_dict))

			special_files = self.gen_files(format_dict=format_dict)
			for filename,content in special_files:
				with open("{local_multijob_dir}/{special_filename}".format(special_filename=filename,**format_dict), "w") as special_file:
					special_file.write(content)

#			with open("{local_multijob_dir}/script.py".format(**format_dict), "w") as script_file:
#				script_file.write(self.multijob_script(format_dict=format_dict))
#
#			with open("{local_multijob_dir}/epilogue.sh".format(**format_dict), "w") as epilogue_file:
#				epilogue_file.write(self.multijob_epilogue(format_dict=format_dict))

			for job in j_list:
				format_dict_job = self.format_dict(job)
				job.multijob_dir = format_dict['multijob_dir']
				#if not os.path.exists(format_dict_job['local_job_dir']):
				#	os.makedirs(format_dict_job['local_job_dir'])
				#session.create_path("{job_dir}".format(**format_dict_job))
				for f in job.files:
					#session.put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))
					session.batch_put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))

			for f,c in special_files:
				st = os.stat(os.path.join(format_dict['local_multijob_dir'],f))
				os.chmod(os.path.join(format_dict['local_multijob_dir'],f), st.st_mode | stat.S_IXUSR)
				session.put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
				session.batch_put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
			session.batch_send(untar_basedir=self.basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=self.command_asjob_output)

			#session.command_output('chmod u+x {multijob_dir}/epilogue.sh'.format(**format_dict))
			#session.command_output('chmod u+x {multijob_dir}/script.py'.format(**format_dict))


			JOBID = self.send_submit_command(cmd_type='multijob',format_dict=format_dict)
			JOBID = self.jobid_from_submit_output(JOBID)
			#session.close()
			if JOBID:
				for i in range(len(j_list)):
					job = j_list[i]
					job.JOBID = self.array_jobid(jobid=JOBID,jobN=i+1)
					job.status = 'running'
					job.array_id = i+1
					job.save()
			else:
				raise Exception('No JOBID returned by submit command, multijob '+str(format_dict['multijob_dir']))
				#for i in range(len(j_list)):
				#	job = j_list[i]
				#	job.save()
		self.waiting_to_submit = {}

	def check_running_jobs(self):
		self.finished_running_jobs = []
		session = self.ssh_session
		running_jobs_string = self.get_running_jobs_string()
		for j in self.job_list:
			if j.status == 'running' and running_jobs_string.find(j.JOBID) == -1:
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

	def individual_retrieve_job(self, job):
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
		session.batch_get(job_dir, local_job_dir)
		session.batch_receive(untar_basedir=self.local_basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=self.command_asjob_output)
		#session.get_dir(job_dir, local_job_dir)

		#session.close()
		job.update()
		job.path = path

	def retrieve_job(self,job):
		if not hasattr(self,'to_remove'):
			self.to_remove = []
		path = copy.deepcopy(job.path)
		session = self.ssh_session
		if job.uuid in self.backups_status['present'] and not job.uuid in self.backups_status['locked']:
			job_dir = self.format_dict(job)['job_backup_dir']
		else:
			job_dir = self.format_dict(job)['job_dir']
		local_job_dir = self.format_dict(job)['local_job_dir']
		if hasattr(job,'clean_at_retrieval'):
			for f in job.clean_at_retrieval:
				self.to_remove.append(os.path.join(job_dir,f))
		session.batch_get(job_dir, local_job_dir)
		if hasattr(job,'multijob_dir'):
			for f in ['output.txt','error.txt']:
				remote_path = os.path.join(job.multijob_dir,f+'-'+str(job.array_id))
				local_path = os.path.join(local_job_dir,f)
				session.batch_get(remote_path,local_path)
				#session.command_output('cp {multijob_dir}/{f}-{array_id} {dir}/{f}'.format(f=f,dir=job_dir,multijob_dir=job.multijob_dir,array_id=job.array_id,**self.format_dict(job)))
		#session.batch_receive(untar_basedir=self.local_basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=self.command_asjob_output)
		#session.get_dir(job_dir, local_job_dir)

		#session.close()
		job.status = 'retrieving'

	def global_retrieval(self):
		session = self.ssh_session
		retrieving_list = [j for j in self.job_list if j.status == 'retrieving']
		path_list = []
		for j in retrieving_list:
			path_list.append(copy.deepcopy(j.path))

		if hasattr(self,'to_remove') and len(self.to_remove)>0:
			rm_command = 'rm -R ' + ' '.join(self.to_remove)
			session.command_output(rm_command)
			self.to_remove = []
		session.batch_receive(untar_basedir=self.local_basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=self.command_asjob_output)
		for i in range(len(retrieving_list)):
			j = retrieving_list[i]
			jpath = path_list[i]
			j.update()
			j.path = jpath
			j.close_connections()
		return retrieving_list

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
			out = self.command_asjob_output(' && '.join(cmd),retry=True)
		#session.close()

	def update_virtualenv(self, virtual_env=None, requirements=[],src_path=None):
		cmd = []
		if src_path is None:
			src_path = self.basedir+'/src_'+self.uuid
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
			out = self.command_asjob_output(' && '.join(cmd),retry=True)
			#session.close()

	def command_asjob_output(self,cmd,t_min=10,retry=True,retry_time=30):
		cmd_uuid = str(uuid.uuid1())
		cmd_path = os.path.join(self.basedir,'tempcommand_'+cmd_uuid)
		out = self.ssh_session.command_output('mkdir -p '+cmd_path)
		file_path = os.path.join(cmd_path,'cmd.sh')
		output_path = os.path.join(cmd_path,'output.txt')
		self.ssh_session.command_output('echo \"#!/bin/bash\n'+cmd+'\" > '+file_path+' && chmod u+x '+file_path)
		cmdjob_id = self.send_submit_command(cmd_type='simple',t_min=t_min,output_path=output_path, file_path=file_path)
		cmdjob_id = self.jobid_from_submit_output(cmdjob_id)
		t = time.time()
		running_jobs_string = self.get_running_jobs_string()
		#while not self.ssh_session.path_exists(output_path):
		while running_jobs_string.find(cmdjob_id) != -1:
			time.sleep(5)
			#if time.time()-t > 60*t_min:
			running_jobs_string = self.get_running_jobs_string()
		output = self.ssh_session.command_output('cat '+output_path)
		if self.output_killed_string() in output:
			if retry:
				return self.command_asjob_output(cmd,t_min=retry_time,retry=False)
			else:
				raise Exception('Command is taking too long, might be blocked')
		else:
			return output
		#self.ssh_session.command_output('rm -R '+cmd_path)


	def cancel_job(self, job, clean=False):
		if job.status == 'running':
			#session = SSHSession(**self.ssh_cfg)
			session = self.ssh_session
			cmd = self.cancel_command(jobid=job.JOBID)
			session.command_output(cmd)
			#session.close()
		super(ClusterJobQueue, self).cancel_job(job, clean=clean)

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
		Njobs = self.count_running_jobs()
		self.available_workers = self.max_jobs - Njobs
		#session.close()

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		del out_dict['ssh_session']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		self.ssh_session = SSHSession(**self.ssh_cfg)

	def gen_files(self, format_dict):
		return []

	def send_submit_command(self,cmd_type,format_dict,t_min,output_path,file_path):
		return []

	def get_running_jobs_string(self):
		return ''

	def count_running_jobs(self):
		return 0

	def cancel_command(self,jobid):
		return ''

	def array_jobid(self,jobid,jobN):
		return jobid+str(jobN)

	def output_killed_string(self):
		return 'Job killed'

	def jobid_from_submit_output(self,submit_output):
		return submit_output
