import uuid
try:
	import cPickle as pickle
except ImportError:
	import pickle
import bz2
import time
import random
import os
import sys
import copy
import shutil
import jsonpickle
import glob
import hashlib
import cProfile, pstats
try:
	from StringIO import StringIO
except ImportError:
	from io import StringIO
import path as pathpy
from memory_profiler import memory_usage
import numpy as np

jsonpickle.set_preferred_backend('json')
jsonpickle.set_encoder_options('json', indent=4)
jsonpickle.enable_fallthrough(False)

def get_md5(filename):
	with open(filename,'rb') as f:
		content = f.read()
	m = hashlib.md5()
	m.update(content)
	return m.hexdigest()

class Job(object):

	def __init__(self, descr=None, virtual_env=None, requirements=[], estimated_time=2*3600, max_time=48*3600, path = 'jobs', erase=False, profiling=False, checktime=False, seeds=None, get_data_at_unpack=False,*args,**kwargs):
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
		self.init_path = path
		self.path = os.path.join(path,self.job_dir)
		self.estimated_time = estimated_time
		self.profiling = profiling
		self.memory_usage = []
		self.mem_max = 0.
		self.deps = []
		self.prg_seeds = {'random':random.randint(0, sys.maxsize), 'numpy':random.randint(0, 4294967295)}
		if seeds is not None:
			self.prg_seeds.update(seeds)
		random.seed(self.prg_seeds['random'])
		np.random.seed(self.prg_seeds['numpy'])
		self.prg_states = {'random':random.getstate(), 'numpy':np.random.get_state()}
		self.data = None
		#self.save()
		#self.data = None
		self.backup_dir = os.path.join('..','backup_dir')
		self.python_version = sys.version_info[0]
		self.files_md5 = {}
		self.init(*args,**kwargs)
		self.save(keep_data=False)
		if hasattr(self,'close_connections'):
			self.close_connections()

	def init(self,*args,**kwargs):
		pass

	def update_md5(self,chdir=False):
		if chdir:
			j_path = self.get_path()
		else:
			j_path = '.'
		self.files_md5 = {}
		with pathpy.Path(j_path):
			for f in self.files:
				if f not in  ['job.json']:
					if os.path.isfile(f):
						self.files_md5[f] = get_md5(f)

	def check_md5(self,chdir=False,bool_mode=False):
		if chdir:
			j_path = self.get_path()
		else:
			j_path = '.'
		with pathpy.Path(j_path):
			for f in list(self.files_md5.keys()):
				if not os.path.isfile(f):
					if bool_mode:
						return False
					else:
						raise IOError('File '+str(f)+' not present, should have md5 '+str(self.files_md5[f]))
				elif not self.files_md5[f] == get_md5(f):
					if bool_mode:
						return False
					else:
						raise IOError('File '+str(f)+' had md5 '+str(get_md5(f))+' but should have md5 '+str(self.files_md5[f]))
		if bool_mode:
			return True

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
			with open('prg_states.b','wb') as f:
				f.write(pickle.dumps(self.prg_states, pickle.HIGHEST_PROTOCOL))

	def load_prg_states(self):
		with open('prg_states.b','rb') as f:
			self.prg_states = pickle.loads(f.read())

	def set_prg_states(self):
		random.setstate(self.prg_states['random'])
		np.random.set_state(self.prg_states['numpy'])

	def run(self):
		self.lastsave_time = -1
		with pathpy.Path(self.get_path()):
			self.check_md5()
			self.status = 'unfinished'
			self.init_time += time.time()
			self.save(chdir=False)
			try:
				if os.path.isfile('profile.txt'):
					os.remove('profile.txt')
				self.start_profiler()
				if not hasattr(self, 'prg_states'):
					self.load_prg_states()
				self.set_prg_states()
			except:
				self.status = 'missubmitted'
				self.save(chdir=False)
				raise
			try:
				self.get_data()
				self.script()
				self.save_data()
			except Exception as e:
				with open('scripterror_notifier','w') as f:#directly change job status and save, then raise?
					f.write(str(e)+'\n')
				raise
			self.get_prg_states()
			self.stop_profiler()
			self.save_profile()
			self.update_exec_time()
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
			s = StringIO()
			sortby = 'tottime'#'cumulative'
			try:
				ps = pstats.Stats(self.profiler, stream=s).sort_stats(sortby)
			except:
				ps = pstats.Stats(self.profiler, stream=s)
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
				t = 4*self.estimated_time/10.
			self.update_exec_time()
			if self.lastsave_time == -1 or ((self.exec_time + self.init_time) - self.lastsave_time > t):
				self.get_prg_states()
				self.check_mem()
				self.save_profile()
				self.save(chdir=False,backup=True)

	def check_mem(self):
		mem = memory_usage()
		if isinstance(mem,list):
			mem = mem[0]
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
					self.estimated_time = int(min(self.estimated_time*1.1, self.max_time))
				else:
					self.estimated_time = int(min(self.estimated_time*2, self.max_time))
				filelist = [txtfile for txtfile in glob.glob('*.txt') if not (len(txtfile)>=8 and txtfile[-8:]!='_old.txt')]
				for txtfile in filelist:
					with open(txtfile,'r') as f_out:
						with open(txtfile[:-4]+'_old.txt','a') as f_in:
							f_in.write('\n=========================' + time.strftime("[%Y %m %d %H:%M:%S]", time.localtime()) + '=========================\n')
							f_in.write(f_out.read())
					os.remove(txtfile)
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
			self.update_md5()
			with open('job.json','w') as f:
				f.write(jsonpickle.dumps(self))#,pickle.HIGHEST_PROTOCOL))
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
		os.makedirs(own_backup_dir)
		for f in self.files:
			if os.path.isfile(f):
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
				#if not self.check_md5(bool_mode=True):
				#	self.status = 'md5 check failed'
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

	def save_data(self,data=None):
		pass

	def get_data(self):
		pass

	def script(self):
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
		if 'prg_states' in list(out_dict.keys()):
			del out_dict['prg_states']
		if 'profiler' in list(out_dict.keys()):
			del out_dict['profiler']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)
		#self.load_prg_states() # jsonpickle.loads has to be executed in the jobdir, otherwise prg states file is not found.


	def restart(self):
		pass

	def move(self, new_path):
		complete_new_path = os.path.join(new_path,self.job_dir)
		if not os.path.exists(new_path):
			os.makedirs(new_path)
		if os.path.exists(self.path):
			shutil.move(self.path, os.path.join(new_path))
		self.init_path = new_path
		self.path = complete_new_path
