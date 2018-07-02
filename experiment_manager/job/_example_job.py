
from . import Job

import time
import json
import os
import shutil
import random

class ExampleJob(Job):

	def init(self,*args,**kwargs):
		if os.path.isfile(self.descr+'data.dat'):
			try:
				os.makedirs(self.path)
			except OSError as exc:
				if exc.errno == errno.EEXIST and os.path.isdir(self.path):
					pass
				else:
					raise
			shutil.copy(self.descr+'data.dat', self.path+'/'+self.descr+'data.dat')

	def script(self):
		for i in range(0,6):
			r = random.random()
			for j in range(0,4):
				self.data+=1
				print(self.data)
				time.sleep(1+r)
			self.check_time()

	def get_data(self):
		if os.path.isfile(self.descr+'data.dat'):
			with open(self.descr+'data.dat','r') as f:
				self.data = json.loads(f.read())
		else:
			self.data = 0

	def save_data(self):
		with open(self.descr+'data.dat','w') as f:
			f.write(json.dumps(self.data))

	def unpack_data(self):
		shutil.move(self.path+'/'+self.descr+'data.dat', self.descr+'data.dat')

