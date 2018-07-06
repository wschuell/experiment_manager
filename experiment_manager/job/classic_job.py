
from . import Job

import time
try:
	import cPickle as pickle
except ImportError:
	import pickle
import bz2
import os
import shutil

class ClassicJob(Job):

	def init(self, obj=None, filename='data.dat', run_fun='run', out_files=None, bz2=True, *args,**kwargs):
		self.filename = filename
		self.bz2 = bz2
		self.run_fun = run_fun
		if obj is None:
			self.get_data()
		else:
			self.data = obj
		if out_files is None:
			self.out_files = [self.filename]
		elif not isinstance(out_files,(list,tuple)):
			self.out_files = [out_files]

	def script(self):
		getattr(self.data,self.run_fun)()

	def get_data(self):
		try:
			with open(self.filename,'rb') as f:
				self.data = pickle.loads(f.read())
				self.bz2=False
		except pickle.UnpicklingError:
			with open(self.filename,'rb') as f:
				self.data = pickle.loads(bz2.decompress(f.read()))
				self.bz2 = True
		except Exception:
			raise Exception

	def save_data(self):
		with open(self.filename,'wb') as f:
			if self.bz2:
				f.write(bz2.compress(pickle.dumps(self.data)))
			else:
				f.write(pickle.dumps(self.data))

	def unpack_data(self):
		for f in self.out_files:
			shutil.copy(os.path.join(self.path,f), f)


class IteratedJob(ClassicJob):

	def init(self, obj=None, steps=1, step_fun='step', *args,**kwargs):
		super(IteratedJob,self).init(obj=obj, *args,**kwargs)
		self.steps = steps
		self.step_fun = step_fun

	def script(self):
		while self.steps > 0:
			getattr(self.data,self.step_fun)()
			self.steps -= 1
			self.check_time()

