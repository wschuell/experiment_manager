import uuid
import cPickle
import bz2
import time
import os
import copy
import shutil
import jsonpickle
import cProfile, pstats, StringIO
import path
from memory_profiler import memory_usage

class Job(object):

	def __init__(self, descr='', virtual_env=None, requirements=[], estimated_time=600, max_time=48*3600, path = 'jobs', erase=True, profiling=True):
		self.uuid = str(uuid.uuid1())
		self.status = 'pending'
		self.descr = descr
		self.erase = erase
		self.virtual_env = virtual_env
		self.requirements = requirements
		self.init_time = 0.
		self.exec_time = 0.
		if path[0] == '/':
			raise IOError('path must be relative')
		self.job_dir = '_'.join([time.strftime('%Y-%m-%d_%H-%M-%S'), self.descr, self.uuid])
		self.path = os.path.join(path,self.job_dir)
		self.estimated_time = estimated_time
		self.profiling = profiling
		self.memory_usage = []
		self.mem_max = None
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

	def run(self):
		with path.Path(self.get_path()):
			self.status = 'unfinished'
			self.init_time += time.time()
			if self.profiling:
				pr = cProfile.Profile()
				pr.enable()
			self.get_data()
			self.script()
			if self.profiling:
				pr.disable()
				s = StringIO.StringIO()
				sortby = 'time'
				ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
				ps.print_stats()
				with open('profile.txt','w') as f:
					f.write(s.getvalue())
			self.update_exec_time()
			self.save_data()
		self.status = 'done'
		self.save()

	def update_exec_time(self):
		self.exec_time = time.time() - self.init_time

	def check_time(self, t=None):
		self.check_mem()
		if t is None:
			t = self.estimated_time/10
		self.update_exec_time()
		if (self.exec_time + self.init_time) - self.lastsave_time > t:
			self.save()

	def check_mem(self):
		mem = memory_usage()
		self.memory_usage.append(mem)
		self.mem_max = max(mem,self.mem_max)

	def fix(self):
		if self.exec_time > 0:
			self.init_time = -self.exec_time
		else:
			if self.estimated_time >= self.max_time:
				raise Exception('JobError: Job is too long, consider saving it while running! Command check_time() does it, depending wisely on execution time.')
			self.estimated_time = min(self.estimated_time*2, self.max_time)
		self.status = 'pending'

	def save(self):
		if self.data is not None:
			with path.Path(self.get_path()):
				self.save_data()
		tempdata = copy.deepcopy(self.data)
		self.data = None
		if not os.path.exists(self.path):
			os.makedirs(self.path)
		with open(self.path+'/job.json','w') as f:
			jsonpickle.set_encoder_options('simplejson', sort_keys=True, indent=4)
			f.write(jsonpickle.dumps(self))#,cPickle.HIGHEST_PROTOCOL))
		self.data = tempdata
		self.lastsave_time = time.time()

	def clean(self):
		shutil.rmtree(self.path)
		head, tail = os.path.split(self.path)
		if not os.listdir(head):
			shutil.rmtree(head)

	def update(self):
		if os.path.isfile(self.path + '/job.json'):
			with open(self.path + '/job.json') as f:
				out_job = jsonpickle.loads(f.read())
			self.__dict__.update(out_job.__dict__)
		else:
			self.save()

	def __eq__(self, other):
		if (self.__class__ == other.__class__):
			k1 = self.__dict__.keys()
			k2 = other.__dict__.keys()
			#k1.remove('uuid')
			#k2.remove('uuid')
			return set(k1) == set(k2) and all(self.__dict__[k] == other.__dict__[k] for k in k1)
		else:
			return False

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
