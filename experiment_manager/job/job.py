import uuid
import cPickle
import bz2
import time
import os
import copy
import shutil


class Job(object):

	def __init__(self, descr='', virtual_env=None, estimated_time=600, max_time=48*3600, path = 'jobs'):
		self.uuid = str(uuid.uuid1())
		self.status = 'pending'
		self.descr = descr
		self.virtual_env = virtual_env
		self.init_time = 0.
		self.exec_time = 0.
		if path[0] == '/':
			raise IOError('path must be relative')
		self.job_dir = '_'.join([time.strftime('%Y-%m-%d_%H-%M-%S'), self.descr, self.uuid])
		self.path = os.path.join(path,self.job_dir)
		self.estimated_time = estimated_time
		self.data = None
		self.save()

	def get_path(self):
		if os.path.exists(self.path):
			return self.path
		else:
			return '.'

	def get_back_path(self):
		if os.path.exists(self.path):
			return os.path.join(['..']*len(self.path.split('/')))
		else:
			return '.'

	def run(self):
		os.chdir(self.get_path())
		self.status = 'unfinished'
		self.init_time += time.mktime(time.gmtime())
		self.get_data()
		self.script()
		self.update_exec_time()
		self.save_data()
		os.chdir(self.get_back_path())
		self.save()
		self.status = 'done'

	def update_exec_time(self):
		self.exec_time = time.mktime(time.gmtime()) - self.init_time

	def check_time(self, t=None):
		if t is None:
			t = self.estimated_time/10
		self.update_exec_time()
		if (self.exec_time + self.init_time) - self.lastsave_time > t:
			self.save()

	def fix(self):
		if self.exec_time > 0:
			self.init_time = -self.exec_time
			print 'fix situation1'
		else:
			if self.estimated_time == self.max_time:
				raise Exception('JobError: Job is too long, consider saving it while running!')
			self.estimated_time = min(self.estimated_time*2, self.max_time)
			print 'fix situation2'
		self.status = 'pending'

	def save(self):
		if self.data is not None:
			os.chdir(self.get_path())
			self.save_data()
		tempdata = copy.deepcopy(self.data)
		self.data = None
		if not os.path.exists(self.path):
			os.makedirs(self.path)
		with open(self.path+'/job.b','w') as f:
			f.write(cPickle.dumps(self,cPickle.HIGHEST_PROTOCOL))
		self.data = tempdata
		self.lastsave_time = time.mktime(time.gmtime())

	def clean(self):
		shutil.rmtree(self.path)
		head, tail = os.path.split(self.path)
		if not os.listdir(head):
			shutil.rmtree(head)

	def prepare(self):
		self.save()
		self.pack_data()

	def update(self):
		if os.path.isfile(self.path + '/job.b'):
			with open(self.path + '/job.b') as f:
				out_job = cPickle.loads(f.read())
			self.__dict__.update(out_job.__dict__)

	def pack_data(self):
		pass

	def unpack_data(self):
		pass

	def save_data(self,data):
		pass

	def get_data(self):
		pass

	def script(self, data):
		pass

#make data files
#self.data_file_list
#reinsert_data



#		if db:
#			self.db = db
#		else:
#			self.db = Database(path='{}_{}.db'.format(self.descr, self.uuid))
