import uuid
import cPickle
import bz2
import time
import random
import os
import sys
import copy
import shutil
import jsonpickle
import glob
import cProfile, pstats, StringIO
import path as pathpy
from memory_profiler import memory_usage
import numpy as np

jsonpickle.set_preferred_backend('json')
jsonpickle.set_encoder_options('json', indent=4)


class Job(object):

	def __init__(self, descr=None, virtual_env=None, requirements=[], estimated_time=3600, max_time=48*3600, path = 'jobs', erase=False, profiling=False, checktime=False, seeds=None, get_data_at_unpack=True):
		self.uuid = str(uuid.uuid1())
		self.status = 'pending'
		if descr is None:
			self.descr = self.__class__.__name__
		else:
			self.descr = descr
		self.files = ['job.json','prg_states.b']
		self.get_data_at_unpack = get_data_at_unpack
		self.erase = erase
		self.virtual_env = virtual_env
		self.requirements = requirements
		self.init_time = 0.
		self.exec_time = 0.
		self.max_time = max_time
		self.checktime = checktime
		if path[0] == '/':
			raise IOError('path must be relative')
		self.job_dir = '_'.join([time.strftime('%Y-%m-%d_%H-%M-%S'), self.descr, self.uuid])
		self.path = os.path.join(path,self.job_dir)
		self.estimated_time = estimated_time
		self.profiling = profiling
		self.memory_usage = []
		self.mem_max = 0.
		self.deps = []
		self.prg_seeds = {'random':random.randint(0, sys.maxint), 'numpy':random.randint(0, 4294967295)}
		if seeds is not None:
			self.prg_seeds.update(seeds)
		random.seed(self.prg_seeds['random'])
		np.random.seed(self.prg_seeds['numpy'])
		self.prg_states = {'random':random.getstate(), 'numpy':np.random.get_state()}
		self.data = None
		#self.save()
		#self.data = None
		self.backup_dir = os.path.join('..','backup_dir')

	def get_path(self):
		if not os.path.exists(self.path):
			os.makedirs(self.path)
		return self.path

	def get_back_path(self):
		if self.path == '.':
			return '.'
		else:
			depth = len(os.path.normpath(self.path).split('/'))
			return os.path.join(*(['..']*depth))

	def get_prg_states(self):
		self.prg_states = {'random':random.getstate(), 'numpy':np.random.get_state()}

	def save_prg_states(self):
		if hasattr(self,'prg_states'):
			with open('prg_states.b','w') as f:
				f.write(cPickle.dumps(self.prg_states, cPickle.HIGHEST_PROTOCOL))

	def load_prg_states(self):
		with open('prg_states.b','r') as f:
			self.prg_states = cPickle.loads(f.read())

	def set_prg_states(self):
		random.setstate(self.prg_states['random'])
		np.random.set_state(self.prg_states['numpy'])

	def run(self):
		self.lastsave_time = time.time()
		with pathpy.Path(self.get_path()):
			self.status = 'unfinished'
			self.init_time += time.time()
			if os.path.isfile('profile.txt'):
				os.remove('profile.txt')
			self.start_profiler()
			self.get_data()
			if not hasattr(self, 'prg_states'):
				self.load_prg_states()
			self.set_prg_states()
			try:
				self.script()
			except Exception as e:
				with open('scripterror_notifier','w') as f:#directly change job status and save, then raise?
					f.write(str(e))
				raise
			self.get_prg_states()
			self.stop_profiler()
			self.save_profile()
			self.update_exec_time()
			self.save_data()
		self.status = 'done'
		self.save(keep_data=False)
		with pathpy.Path(self.get_path()):
			self.clean_backup()

	def start_profiler(self):
		if self.profiling:
			self.profiler = cProfile.Profile()
			self.profiler.enable()

	def stop_profiler(self):
		if self.profiling and hasattr(self,'profiler'):
			self.profiler.disable()

	def save_profile(self):
		if self.profiling:
			s = StringIO.StringIO()
			sortby = 'tottime'#'cumulative'
			ps = pstats.Stats(self.profiler, stream=s).sort_stats(sortby)
			ps.print_stats()
			with open('profile.txt','a') as f:
				f.write(time.strftime("[%Y %m %d %H:%M:%S]\n", time.localtime()))
				f.write(s.getvalue())
			s.close()

	def update_exec_time(self):
		self.exec_time = time.time() - self.init_time

	def check_time(self, t=None):
		if self.checktime:
			if t is None:
				t = 45*self.estimated_time/100.
			self.update_exec_time()
			if (self.exec_time + self.init_time) - self.lastsave_time > t:
				self.get_prg_states()
				self.check_mem()
				self.save_profile()
				self.save(chdir=False,backup=True)

	def check_mem(self):
		mem = memory_usage()
		self.memory_usage.append(mem)
		self.mem_max = max(mem,self.mem_max)

	def fix(self):
		with pathpy.Path(self.get_path()):
			if glob.glob('scripterror_notifier'):
				self.status = 'script error'
			else:
				if self.estimated_time >= self.max_time:
					raise Exception('JobError: Job is too long, consider saving it while running! Command check_time() does it, depending wisely on execution time.')
				if self.exec_time > 0:
					self.init_time = -self.exec_time
					self.estimated_time = min(self.estimated_time*2, self.max_time)
				else:
					self.estimated_time = min(self.estimated_time*4, self.max_time)
				for txtfile in glob.glob('*.txt'):
					with open(txtfile,'r') as f_out:
						with open(txtfile[:-4]+'_old.txt','a') as f_in:
							f_in.write('\n=========================' + time.strftime("[%Y %m %d %H:%M:%S]", time.localtime()) + '=========================\n')
							f_in.write(f_out.read())
				self.status = 'pending'

	def save(self,chdir=True, keep_data=True, backup=False):
		data_exists = False
		if chdir:
			j_path = self.get_path()
		else:
			j_path = '.'
		if self.data is not None:
			data_exists = True
			with pathpy.Path(j_path):
				self.save_data()
		self.data = None
			#if not os.path.exists(self.path):
			#	os.makedirs(self.path)
		#self.prg_states = {'random':random.getstate()}#, 'numpy':np.random.get_state()}
		self.lastsave_time = time.time()
		with pathpy.Path(j_path):
			self.save_prg_states()
			with open('job.json','w') as f:
				f.write(jsonpickle.dumps(self))#,cPickle.HIGHEST_PROTOCOL))
		if keep_data and data_exists:
			with pathpy.Path(j_path):
				self.get_data()
		if backup:
			with pathpy.Path(j_path):
				self.backup()

	def backup(self):
		backup_dir = self.backup_dir
		own_backup_dir = os.path.join(self.backup_dir,self.uuid)
		backup_lock_dir = os.path.join(self.backup_dir,'backup_lock')
		backup_lock_file = os.path.join(backup_lock_dir,self.uuid)

		if not os.path.isdir(backup_dir):
			os.makedirs(backup_dir)
		if not os.path.isdir(backup_lock_dir):
			os.makedirs(backup_lock_dir)
		with open(backup_lock_file,'w') as f:
			f.write('locked')

		self.clean_backup()
		for f in self.files:
			if not os.path.isdir(os.path.dirname(os.path.join(own_backup_dir,f))):
				os.makedirs(os.path.dirname(os.path.join(own_backup_dir,f)))
			shutil.copy(f,os.path.join(own_backup_dir,f))
		#shutil.copytree('.',own_backup_dir,ignore=shutil.ignore_patterns(own_backup_dir))
		os.remove(backup_lock_file)

	def clean_backup(self):
		own_backup_dir = os.path.join(self.backup_dir,self.uuid)
		shutil.rmtree(own_backup_dir,ignore_errors=True)

	def clean(self):
		self.clean_backup()
		head, tail = os.path.split(self.path)
		if os.path.exists(os.path.join(self.path,'profile.txt')):
			if os.path.exists(os.path.join(self.path,'profile_old.txt')):
				with pathpy.Path(self.get_path()):
					with open('profile.txt','r') as f_out:
						with open('profile_old.txt','a') as f_in:
							f_in.write('\n=========================' + time.strftime("[%Y %m %d %H:%M:%S]", time.localtime()) + '=========================\n')
							f_in.write(f_out.read())
					os.remove('profile.txt')
					shutil.move('profile_old.txt','profile.txt')
			if not os.path.exists(os.path.join(head,'profiles')):
				os.mkdir(os.path.join(head,'profiles'))
			shutil.move(os.path.join(self.path,'profile.txt'),os.path.join(head,'profiles',tail+'.txt'))
		try:
			shutil.rmtree(self.path)
			if not os.listdir(head):
				shutil.rmtree(head)
		except OSError:
			pass

	def update(self):
		if os.path.isfile(self.path + '/job.json'):
			with pathpy.Path(self.path):
				with open('job.json') as f:
					out_job = jsonpickle.loads(f.read())
				self.__dict__.update(out_job.__dict__)
				if os.path.isfile('scripterror_notifier'):
					self.status = 'script error'
		else:
			self.save()

	def __eq__(self, other):
		return self.uuid == other.uuid

	def __gt__(self, other):
		return False

	def __lt__(self, other):
		return False

	def unpack_data(self):
		pass

	def save_data(self,data):
		pass

	def get_data(self):
		pass

	def script(self, data):
		pass

	def re_init(self):
		pass

	def gen_depend(self):
		return []

	def close_connections(self):
		pass


	def get_error(self):
		with pathpy.Path(self.get_path()):
			if os.path.isfile('error.txt'):
				with open('error.txt','r') as f:
					return f.read()
			else:
				return 'Error file doesnt exist or job queue not supporting error management'

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		if 'prg_states' in out_dict.keys():
			del out_dict['prg_states']
		if 'profiler' in out_dict.keys():
			del out_dict['profiler']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		#self.load_prg_states() # jsonpickle.loads has to be executed in the jobdir, otherwise prg states file is not found.

