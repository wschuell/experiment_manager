import uuid
import cPickle
import bz2
import time

def idle(*args,**kwargs):
	pass

class Job(object):

	def __init__(self, descr='', db=None, virtual_env=None, estimated_time=600, max_time=48*3600):
		self.uuid = str(uuid.uuid1())
		self.status = 'pending'
		self.descr = descr
		self.script = script
		self.make_data_file = make_data_file
		self.reinsert_data = reinsert_data
		self.virtual_env = virtual_env
		self.init_time = 0.
		self.exec_time = 0.


	def get_data(self, filename):
		with open(filename, 'r') as f:
			data cPickle.loads(bz2.decompress(f.read()))

	def run():
		self.status = 'unfinished'
		self.init_time += time.mktime(time.gmtime())
		self.script()
		self.update_exec_time()
		self.status = 'done'

	def update_exec_time(self):
		self.exec_time = time.mktime(time.gmtime()) - self.init_time

	def check_time(self, t=None):
		if t is None:
			t = self.estimated_time/10
		self.update_exec_time()
		if self.exec_time - self.lastsave_time > t:
			self.save()

	def fix(self):
		if self.exec_time > 0:
			self.init_time = -self.exec_time
		self.status = 'pending'
		else:
			if self.estimated_time == self.max_time:
				raise Exception('JobError: Job is too long, consider saving it while running!')
			self.estimated_time = min(self.estimated_time*2, self.max_time)
		self.status = 'pending'

	def save(self):
		path = './jobs/'+self.uuid
		if not os.path.exists(path):
			os.makedirs(path)
		with open(path+'/job.b') as f:
			f.write(cPickle.dumps(self,cPickle.HIGHEST_PROTOCOL))

	def pack_data(self):
		self.save()

	def unpack_data(self, erase=True):
		UNPACKDATA
		if erase:
			self.clean()

	def clean(self):
		path = './jobs/'+self.uuid
		shutil.rmtree(path)
		if not os.listdir('./jobs'):
			shutil.rmtree('./jobs')

#make data files
#self.data_file_list
#reinsert_data



#		if db:
#			self.db = db
#		else:
#			self.db = Database(path='{}_{}.db'.format(self.descr, self.uuid))
