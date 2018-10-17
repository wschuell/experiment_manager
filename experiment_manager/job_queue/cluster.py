
import os
import sys
import shutil
import time
import copy
import uuid
import stat
import json

from . import JobQueue
from ..tools.ssh import SSHSession,get_username_from_hostname,check_hostname

class ClusterJobQueue(JobQueue):
	def __init__(self, ssh_cfg={}, basedir='', local_basedir='', requirements=[], max_jobs=1000, max_jobs_total=None, base_work_dir=None, without_epilogue=False, install_as_job=False, modules=[], **kwargs):
		super(ClusterJobQueue,self).__init__(requirements=requirements,**kwargs)
		self.max_jobs = max_jobs
		if max_jobs_total is None:
			self.max_jobs_total = max_jobs
		else:
			self.max_jobs_total = max_jobs_total
		self.modules = modules
		self.ssh_cfg = ssh_cfg
		self.update_needed = False
		self.ssh_session = SSHSession(auto_connect=False,**self.ssh_cfg)
		self.waiting_to_submit = {}
		self.basedir = os.path.join(basedir,'job_queues',self.jobqueue_dir)#basedir #
		self.archivedir = os.path.join(basedir,'archive_job_queues',self.jobqueue_dir)
		self.local_basedir = os.path.join(local_basedir,'job_queues',self.jobqueue_dir)#local_basedir #
		self.remote_backupdir = os.path.join(self.basedir,'backup_dir')
		self.without_epilogue = without_epilogue
		self.install_as_job = install_as_job
		if base_work_dir is None:
			self.base_work_dir = self.basedir
		else:
			self.base_work_dir = base_work_dir
		if len(self.base_work_dir)>1 and self.base_work_dir[-1] == '/':
			self.base_work_dir = self.base_work_dir[:-1]

	def init_connections(self):
		if not self.ssh_session.connected:
			self.ssh_session.connect()

	def get_walltime(self,walltime_seconds):
		wtime = walltime_seconds
		walltime_h = int(wtime/3600)
		wtime -= 3600*walltime_h
		if walltime_h<10:
			walltime_h = '0'+str(walltime_h)
		else:
			walltime_h = str(walltime_h)

		walltime_m = int(wtime/60)
		wtime -= 60*walltime_m
		if walltime_m<10:
			walltime_m = '0'+str(walltime_m)
		else:
			walltime_m = str(walltime_m)

		walltime_s = int(wtime)
		if walltime_s<10:
			walltime_s = '0'+str(walltime_s)
		else:
			walltime_s = str(walltime_s)
		walltime = ':'.join([walltime_h, walltime_m, walltime_s])
		return walltime


	def format_dict(self, job):

		if hasattr(job,'JOBID'):
			jobid = job.JOBID
		else:
			jobid = 'NO_JOBID'

		if job.virtual_env is None:
			if hasattr(self,'python_version'):
				python_bin = '/usr/bin/env python'+str(self.python_version)
			else:
				python_bin = '/usr/bin/env python'
		else:
			python_bin = '/home/{}/virtualenvs/{}/bin/python'.format(self.ssh_session.get_username(), job.virtual_env)

		walltime = self.get_walltime(job.estimated_time)

		if job.optimize:
			optimize = 'export PYTHONOPTIMIZE=1'
		else:
			optimize = ''

		format_dict = {
			'username':self.ssh_session.get_username(),
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
			#'modules_cluster': ' '.join(self.modules),
			'walltime_seconds': job.estimated_time,
			'walltime': walltime,
			'prefix':self.get_prefix(job),
			'optimize':optimize
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
			if os.path.exists(os.path.join(format_dict['local_job_dir'],f)):
				#session.put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
				session.batch_put(os.path.join(format_dict['local_job_dir'],f), os.path.join(format_dict['job_dir'],f))
		session.batch_send(untar_basedir=self.basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=None)#,command_send_func=self.command_asjob_output)
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
		#wt = format_dict['walltime']
		wt = format_dict['prefix']
		if wt not in list(self.waiting_to_submit.keys()):
			self.waiting_to_submit[wt] = []
		self.waiting_to_submit[wt].append(job)

	def global_submit(self):
		session = self.ssh_session
		jobdir_dict = {}
		for wt,j_list in list(self.waiting_to_submit.items()):
			jobdir_dict[wt] = {}
			for i in range(len(j_list)):
				jobdir_dict[wt][i+1] = self.format_dict(j_list[i])['job_dir']

		for wt,j_list in list(self.waiting_to_submit.items()):
			format_dict = self.format_dict(j_list[0])
			multijob_dir = self.name+'_'+self.uuid+'_'+j_list[0].uuid+'_'+time.strftime("%Y%m%d%H%M%S", time.localtime())
			format_dict .update({
				'jobdir_dict':str(jobdir_dict[wt]),
				'jobdir_dict_json':json.dumps(jobdir_dict[wt],sort_keys=True),
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
					if os.path.exists(os.path.join(format_dict_job['local_job_dir'],f)):
						#session.put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))
						session.batch_put(os.path.join(format_dict_job['local_job_dir'],f), os.path.join(format_dict_job['job_dir'],f))

			for f,c in special_files:
				st = os.stat(os.path.join(format_dict['local_multijob_dir'],f))
				os.chmod(os.path.join(format_dict['local_multijob_dir'],f), st.st_mode | stat.S_IXUSR)
				if os.path.exists(os.path.join(format_dict['local_multijob_dir'],f)):
					#session.put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
					session.batch_put(os.path.join(format_dict['local_multijob_dir'],f), os.path.join(format_dict['multijob_dir'],f))
			session.batch_send(untar_basedir=self.basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=None)#,command_send_func=self.command_asjob_output)

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
		presentbackups = session.command_output('ls -l '+self.remote_backupdir,check_exit_code=False)
		lockedbackups = session.command_output('ls -l '+os.path.join(self.remote_backupdir,'backup_lock'),check_exit_code=False)
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
		session.batch_receive(untar_basedir=self.local_basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=None)#,command_send_func=self.command_asjob_output)
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
			cmd_length = len(rm_command)
			max_size_cmd = 100000
			if cmd_length > max_size_cmd:
				to_rm = copy.deepcopy(self.to_remove)
				while to_rm:
					new_cmd = 'rm -R'
					while (len(new_cmd) < max_size_cmd) and to_rm:
						new_cmd += ' ' + to_rm.pop()
					session.command_output(new_cmd,check_exit_code=False)
			else:
				session.command_output(rm_command,check_exit_code=False)
			self.to_remove = []
		session.batch_receive(untar_basedir=self.local_basedir,localtardir=os.path.join(self.local_basedir,'tar_dir'),remotetardir=os.path.join(self.basedir,'tar_dir'),command_send_func=None)#,command_send_func=self.command_asjob_output)
		for i in range(len(retrieving_list)):
			j = retrieving_list[i]
			jpath = path_list[i]
			j.update()
			j.path = jpath
			j.close_connections()
		return retrieving_list

	def set_virtualenv(self, virtual_env, requirements=[], sys_site_packages=True):
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		python_version = str(sys.version_info[0])
		if hasattr(self,'modules') and self.modules:
			session.prefix_command = 'module load '+ ' '.join(self.modules) + ' && '
		#if len(session.command_output('command -v virtualenv')) > 1:
		#	virtualenv_bin = 'virtualenv'
		mod_venv = session.command_output('python'+python_version+' -c "import virtualenv; print(virtualenv);"')
		if not(len(mod_venv)>27 and mod_venv[:27] == "<module 'virtualenv' from '"):
			session.command_output('pip'+python_version+' install --user virtualenv')
			mod_venv = session.command_output('python'+python_version+' -c "import virtualenv; print(virtualenv);"')
		assert (len(mod_venv)>27 and mod_venv[:27] == "<module 'virtualenv' from '")
		if mod_venv[-5:] == "pyc'>":
			mod_venv_clean = mod_venv[27:-4]
		else:
			mod_venv_clean = mod_venv[27:-3]
		virtualenv_bin = 'python'+ python_version + ' ' + mod_venv_clean
		cmd = []
		if sys_site_packages:
			site_pack = '--system-site-packages '
		else:
			site_pack = ''
		if not isinstance(requirements, (list, tuple)):
			requirements = [requirements]
		if virtual_env is None:
			for package in requirements:
				session.command('pip'+python_version+' install --user '+package)
		else:
			if not session.path_exists('/home/{}/virtualenvs/{}'.format(self.ssh_session.get_username(), virtual_env)):
				cmd.append(virtualenv_bin + ' {}/home/{}/virtualenvs/{}'.format(site_pack,self.ssh_session.get_username(), virtual_env))
			cmd.append('source /home/{}/virtualenvs/{}/bin/activate'.format(self.ssh_session.get_username(), virtual_env))
			for package in requirements:
				cmd.append('pip'+ python_version + ' install '+package)
			cmd.append('deactivate')
			#out = session.command_output(' && '.join(cmd))
			if self.install_as_job:
				out = self.command_asjob_output(' && '.join(cmd),retry=True)
			else:
				out = self.ssh_session.command_output(' && '.join(cmd))
		#session.close()

	def update_virtualenv(self, virtual_env=None, requirements=[],src_path=None):
		cmd = []
		if src_path is None:
			src_path = self.basedir+'/src_'+self.uuid #maybe choose something else as jobqueue folder? (because may be used later by other job queues)
		if not isinstance(requirements, (list, tuple)):
			requirements = [requirements]
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		if hasattr(self,'modules') and self.modules:
			session.prefix_command = 'module load '+ ' '.join(self.modules) + ' && '
		if virtual_env is not None and not session.path_exists('/home/{}/virtualenvs/{}'.format(self.ssh_session.get_username(), virtual_env)):
			#session.close()
			self.set_virtualenv(virtual_env=virtual_env, requirements=requirements)
		else:
			if virtual_env is not None:
				cmd.append('source /home/{}/virtualenvs/{}/bin/activate'.format(self.ssh_session.get_username(), virtual_env))
				option=''
				python_version = ''
			else:
				option='--user '
				python_version = str(sys.version_info[0])
			if requirements == ['all']:
					cmd.append("pip"+python_version+" freeze --local | grep -v '^\-e' | cut -d = -f 1  | xargs pip install -U "+option)
			elif len(requirements)>0:
					cmd.append('pip'+python_version+' install --upgrade --no-deps --src '+src_path+' '+option+' '.join(requirements))
					cmd.append('pip'+python_version+' install --src '+src_path+' '+option+' '.join(requirements))
			if virtual_env is not None:
				cmd.append('deactivate')
			#out = session.command_output(' && '.join(cmd))
			if self.install_as_job:
				out = self.command_asjob_output(' && '.join(cmd),retry=True)
			else:
				out = self.ssh_session.command_output(' && '.join(cmd))
			#session.close()

	def command_asjob_output(self,cmd,t_min=10,retry=True,retry_time=30):
		session = self.ssh_session
		if hasattr(self,'modules') and self.modules:
			session.prefix_command = 'module load '+ ' '.join(self.modules) + ' && '
		pref_cmd = session.prefix_command + cmd
		cmd_uuid = str(uuid.uuid1())
		cmd_path = os.path.join(self.basedir,'tempcommand_'+cmd_uuid)
		out = self.ssh_session.command_output('mkdir -p '+cmd_path)
		file_path = os.path.join(cmd_path,'cmd.sh')
		output_path = os.path.join(cmd_path,'output.txt')
		self.ssh_session.command_output('echo \"#!/bin/bash\n'+pref_cmd+'\" > '+file_path+' && chmod u+x '+file_path)
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
		return min(self.available_workers,self.max_jobs) - offset_waiting
		#session.close()

	def refresh_avail_workers(self):
		#session = SSHSession(**self.ssh_cfg)
		session = self.ssh_session
		Njobs = self.count_running_jobs()
		self.available_workers = self.max_jobs_total - Njobs
		#session.close()

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		del out_dict['ssh_session']
		return out_dict

	def __setstate__(self, in_dict):
		JobQueue.__setstate__(self,in_dict)
		self.ssh_session = SSHSession(auto_connect=False,**self.ssh_cfg)

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

	def clean_jobqueue(self):
		session = self.ssh_session
		if hasattr(self,'archivedir'):
			cmd = 'mkdir -p ' + self.archivedir + ' && mv -f ' + self.basedir + '/* '+ self.archivedir + '/'
			session.command_output(cmd,check_exit_code=False)

	def get_username_from_hostname(self,hostname):
		return get_username_from_hostname(hostname)

	def check_hostname(self,hostname):
		return check_hostname(hostname)

	def get_prefix(self,job):
		wtime = self.get_walltime(job.estimated_time)
		if hasattr(job,'needed_resources'):
			return self.prefix_string(walltime=wtime,**job.needed_resources)
		else:
			return self.prefix_string(walltime=wtime)

	def prefix_string(self,walltime):
		return '# walltime='+str(walltime)+'\n'

	def check_python_version(self,virtual_env=None):
		cmd = []
		session = self.ssh_session
		if hasattr(self,'modules') and self.modules:
			session.prefix_command = 'module load '+ ' '.join(self.modules) + ' && '
		if virtual_env is not None:
			python_bin = '/home/{}/virtualenvs/{}/bin/python'.format(self.ssh_session.get_username(), virtual_env)
			if not self.ssh_session.path_exists(python_bin):
				self.set_virtualenv(virtual_env)
		else:
			if hasattr(self,'python_version'):
				python_bin = '/usr/bin/env python'+str(self.python_version)
			else:
				python_bin = '/usr/bin/env python'
		cmd.append(python_bin+' -c "import sys; print(sys.version_info[0]);" ')
		version = session.command_output('&&'.join(cmd))[0]
		if not str(version) == str(sys.version_info[0]):
			raise ValueError("Remote Python version is "+str(version)+", but local Python version is "+str(sys.version_info[0]))
