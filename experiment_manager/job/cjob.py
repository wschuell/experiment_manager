
from . import Job

import subprocess
import os
import shutil
import sys

class CJob(Job):

	def __init__(self, files=[], make_opts=[''], exec_file='',*args,**kwargs):
		Job.__init__(self,*args,**kwargs)
		os.makedirs(self.path)
		for f_src,f_dst in files:
			shutil.copy(f_src,os.path.join(self.path,f_dst))
			self.files.append(f_dst)
		self.make_opts = make_opts
		self.exec_file = exec_file
		self.save()
		self.data = None

	def script(self):
		cmd_tab = []
		for opt in self.make_opts:
			cmd_tab.append('make '+opt)
		cmd_tab.append('./'+self.exec_file)
		cmd = ' && '.join(cmd_tab)
		p = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
		out, err = p.communicate()
		print(out)
		sys.stderr.write(err+'\n')
		exit_code = p.returncode
		if exit_code != 0:
			raise subprocess.CalledProcessError(exit_code,cmd,None)
		#print(subprocess.check_output(cmd.split(' ')))
		#print(subprocess.check_output('make'))
		#print(subprocess.check_output(['./categories']))

#	def unpack_data(self):
#		for f in self.out_files:
#			shutil.copy(os.path.join(self.path,f), f)

#p = subprocess.Popen(cmd, stdout=subprocess.STD_OUTPUT_HANDLE, stderr=subprocess.STD_ERROR_HANDLE)
#stdout, stderr = p.communicate()