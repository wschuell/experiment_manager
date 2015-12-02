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
import cProfile, pstats, StringIO
import path as pathpy
from memory_profiler import memory_usage
import numpy as np

jsonpickle.set_preferred_backend('json')
jsonpickle.set_encoder_options('json', indent=4)


class Job(object):

	def __init__(self, descr='', virtual_env=None, requirements=[], estimated_time=1200, max_time=48*3600, path = 'jobs', erase=True, profiling=True, seeds=None):
		self.uuid = str(uuid.uuid1())
		self.status = 'pending'
		self.descr = descr
		self.erase = erase
		self.virtual_env = virtual_env
		self.requirements = requirements
		self.init_time = 0.
		self.exec_time = 0.
		self.max_time = max_time
		if path[0] == '/':
			raise IOError('path must be relative')
		self.job_dir = '_'.join([time.strftime('%Y-%m-%d_%H-%M-%S'), self.descr, self.uuid])
		self.path = os.path.join(path,self.job_dir)
		self.estimated_time = estimated_time
		self.profiling = profiling
		self.memory_usage = []
		self.mem_max = 0.
		self.deps = []
		self.prg_seeds = {'random':random.randint(0, sys.maxint), 'numpy':random.randint(0, sys.maxint)}
		if seeds is not None:
			self.prg_seeds.update(seeds)
		random.seed(self.prg_seeds['random'])
		np.random.seed(self.prg_seeds['numpy'])
		with pathpy.Path(self.get_path()):
			self.save_prg_states()
		self.data = None
		#self.save()
		#self.data = None

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

	def save_prg_states(self):
		self.prg_states = {'random':random.getstate(), 'numpy':np.random.get_state()}
		with open('prg_states.b','w') as f:
			f.write(cPickle.dumps(self.prg_states, cPickle.HIGHEST_PROTOCOL))

	def load_prg_states(self):
		with open('prg_states.b','r') as f:
			self.prg_states = cPickle.loads(f.read())
		random.setstate(self.prg_states['random'])
		np.random.set_state(self.prg_states['numpy'])

	def run(self):
		self.lastsave_time = time.time()
		with pathpy.Path(self.get_path()):
			self.status = 'unfinished'
			self.init_time += time.time()
			self.start_profiler()
			self.get_data()
			self.load_prg_states()
			self.script()
			self.save_prg_states()
			self.stop_profiler()
			self.save_profile()
			self.update_exec_time()
			self.save_data()
		self.status = 'done'
		self.save()

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
			sortby = 'cumulative'
			ps = pstats.Stats(self.profiler, stream=s).sort_stats(sortby)
			ps.print_stats()
			with open('profile.txt','w') as f:
				f.write(s.getvalue())
			s.close()

	def update_exec_time(self):
		self.exec_time = time.time() - self.init_time

	def check_time(self, t=None):
		if t is None:
			t = self.estimated_time/10
		self.update_exec_time()
		if (self.exec_time + self.init_time) - self.lastsave_time > t:
			self.check_mem()
			self.save_prg_states()
			self.save_profile()
			self.save(chdir=False)

	def check_mem(self):
		mem = memory_usage()
		self.memory_usage.append(mem)
		self.mem_max = max(mem,self.mem_max)

	def fix(self):
		if self.estimated_time >= self.max_time:
			raise Exception('JobError: Job is too long, consider saving it while running! Command check_time() does it, depending wisely on execution time.')
		if self.exec_time > 0:
			self.init_time = -self.exec_time
			self.estimated_time = min(self.estimated_time*1.2, self.max_time)
		else:
			self.estimated_time = min(self.estimated_time*2, self.max_time)
		self.status = 'pending'

	def save(self,chdir=True, keep_data=True):
		data_exists = False
		if chdir:
			j_path = self.get_path()
		else:
			j_path = '.'
		if self.data is not None:
			data_exists = True
			with pathpy.Path(j_path):
				self.save_data()
		self.prg_states = None
		self.data = None
			#if not os.path.exists(self.path):
			#	os.makedirs(self.path)
		#self.prg_states = {'random':random.getstate()}#, 'numpy':np.random.get_state()}
		self.lastsave_time = time.time()
		with pathpy.Path(j_path):
			with open('job.json','w') as f:
				f.write(jsonpickle.dumps(self))#,cPickle.HIGHEST_PROTOCOL))
		if keep_data and data_exists:
			with pathpy.Path(j_path):
				self.get_data()

	def clean(self):
		try:
			shutil.rmtree(self.path)
			head, tail = os.path.split(self.path)
			if not os.listdir(head):
				shutil.rmtree(head)
		except OSError:
			pass

	def update(self):
		if os.path.isfile(self.path + '/job.json'):
			with open(self.path + '/job.json') as f:
				out_job = jsonpickle.loads(f.read())
			self.__dict__.update(out_job.__dict__)
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

	def get_error(self):
		with pathpy.Path(self.get_path()):
			if os.path.isfile('error.txt'):
				with open('error.txt','r') as f:
					return f.read()
			else:
				'Error file doesnt exist or job queue not supporting error management'

	def __getstate__(self):
		out_dict = self.__dict__.copy()
		if 'profiler' in out_dict.keys():
			del out_dict['profiler']
		return out_dict

	def __setstate__(self, in_dict):
		self.__dict__.update(in_dict)

