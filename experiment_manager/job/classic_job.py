
from . import Job

import time
import cPickle
import bz2
import os
import shutil

class ClassicJob(Job):

	def __init__(self,filename='data.dat', run_fun='run', out_files=None, *args,**kwargs):
		super(ExampleJob,self).__init__(*args,**kwargs)
		self.filename = filename
		if out_files is None:
			self.out_files = [self.filename]
		elif not isinstance(out_files,(list,tuple)):
			self.out_files = [out_files]
		if os.path.isfile(self.filename):
			shutil.copy(self.filename, self.path+'/'+self.filename)

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

	def save_data(self):
		with open(self.filename,'w') as f:
			if self.bz2:
				f.write(cPickle.dumps(bz2.compress(self.data)))
			else:
				f.write(cPickle.dumps(self.data))

	def unpack_data(self):
		for f in self.out_files:
			shutil.copy(os.path.join(self.path,f), f)


class IteratedJob(ClassicJob):

	def __init__(self,steps=1, step_fun='step', filename='data.dat',*args,**kwargs):
		super(ClassicJob,self).__init__(filename=filename,*args,**kwargs)
		self.steps = steps

	def script(self):
		for i in range(steps):
			getattr(self.data,self.step_fun)()
			self.check_time()

