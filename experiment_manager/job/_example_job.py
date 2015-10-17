
from. import Job

import time
import json
import os
import shutil

class ExampleJob(Job):

	def script(self):
		for i in range(0,6):
			for j in range(0,10):
				self.data+=1
				time.sleep(1)
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

	def pack_data(self):
		if os.path.isfile(self.descr+'data.dat'):
			shutil.copy(self.descr+'data.dat', self.path+'/'+self.descr+'data.dat')
