
from . import Job

import time
import cPickle
import bz2
import os
import shutil

class ClassicJob(Job):

	def __init__(self, obj=None, filename='data.dat', run_fun='run', out_files=None, bz2=True, *args,**kwargs):
		super(ClassicJob,self).__init__(*args,**kwargs)
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
		self.data = None
		self.save()

	def script(self):
		getattr(self.data,self.run_fun)()

	def get_data(self):
		try:
			with open(self.filename,'r') as f:
				self.data = cPickle.loads(f.read())
				self.bz2=False
		except cPickle.UnpicklingError:
			with open(self.filename,'r') as f:
				self.data = cPickle.loads(bz2.decompress(f.read()))
				self.bz2 = True
		except Exception:
			raise Exception

	def save_data(self):
		with open(self.filename,'w') as f:
			if self.bz2:
				f.write(bz2.compress(cPickle.dumps(self.data)))
			else:
				f.write(cPickle.dumps(self.data))

	def unpack_data(self):
		for f in self.out_files:
			shutil.copy(os.path.join(self.path,f), f)


class IteratedJob(ClassicJob):

	def __init__(self, obj=None, steps=1, step_fun='step', *args,**kwargs):
		super(IteratedJob,self).__init__(obj=obj, *args,**kwargs)
		self.steps = steps
		self.step_fun = step_fun
		self.data = None
		self.save()

	def script(self):
		for i in range(self.steps):
			getattr(self.data,self.step_fun)()
			self.check_time()

