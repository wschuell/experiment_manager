
from. import Job

import time
import os
import shutil
import cPickle
import copy

class ExperimentJob(Job):

	def __init__(self, exp, T, *args,**kwargs):
		super(ExperimentJob, self).__init__(*args,**kwargs)
		self.data = copy.deepcopy(exp)
		self.xp_uuid = self.data.uuid
		self.T = T
		self.save()
		self.data = None

	def script(self):
		for i in range(self.data._T[-1],self.T):
			self.data.continue_exp(self.data.step)
			self.check_time()

	def get_data(self):
		with open(self.xp_uuid+'.b','r') as f:
			self.data = cPickle.loads(f.read())

	def save_data(self):
		with open(self.data.uuid+'.b','w') as f:
			f.write(cPickle.dumps(self.data))

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data.uuid+'.b', self.data.uuid+'_'+self.uuid+'.b')




class ExperimentDBJob(ExperimentJob):

	def __init__(self, exp, T, db_cfg={}, **kwargs):
		super(ExperimentJob, self).__init__(**kwargs)
		self.data = copy.deepcopy(exp)
		self.T = T
		self.origin_db = copy.deepcopy(self.data.db)
		self.db = self.data.db.__class__(**db_cfg)
		self.data.db = self.db
		self.xp_uuid = self.data.uuid
		self.save()
		self.data = None

	def script(self):
		for i in range(self.data._T[-1],self.T):
			self.data.continue_exp(self.data.step, autocommit=False)
			self.check_time()

	def get_data(self):
		self.db.get_experiment(uuid=self.xp_uuid)

	def save_data(self):
		self.data.commit_to_db()

	def unpack_data(self):
		self.data.db = self.origin_db
		self.data.commit_to_db()



class GraphExpJob(ExperimentJob):

	def __init__(self, exp, graph_cfg, **kwargs):
		super(ExperimentJob, self).__init__(**kwargs)
		self.data = {}
		self.graph_filename = None
		self.data['exp'] = copy.deepcopy(exp)
		self.graph_cfg = graph_cfg
		if 'tmax' not in graph_cfg:
			self.graph_cfg['tmax'] = self.data['exp'].-T[-1]
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		self.save()
		self.data = None

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		graph_cfg['tmin'] = self.graph_cfg['tmin']-0.1 - self.data['exp']._time_step
		graph_cfg['tmax'] = self.graph_cfg['tmin']-0.1
		while graph_cfg['tmax']<self.graph_cfg['tmax']:
			graph_cfg['tmax'] += self.data['exp']._time_step
			graph_cfg['tmin'] += self.data['exp']._time_step
			if 'graph' not in self.data.keys():
				self.data['graph'] = self.data['exp'].graph(**graph_cfg)
				self.graph_filename = self.data['graph'].filename
			else:
				self.data['graph'].complete_with(self.data['exp'].graph(**graph_cfg))
			self.check_time()

	def get_data(self):
		with open(self.xp_uuid+'.b','r') as f:
			self.data['exp'] = cPickle.loads(f.read())
		if self.graph_filename is not None and os.path.isfile(self.graph_filename+'.b'):
			with open(self.graph_filename, 'r') as f:
				self.data['graph'] = cPickle.loads(f.read())

	def save_data(self):
		with open(self.data['exp'].uuid+'.b','w') as f:
			f.write(cPickle.dumps(self.data))
		if 'graph' in self.data.keys():
			self.data['graph'].write_files()

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data['exp'].uuid+'.b', self.data['exp'].uuid+'_'+self.uuid+'.b')



class GraphExpDBJob(ExperimentDBJob):

	def __init__(self, exp, **graph_cfg):
		super(ExperimentJob, self).__init__()
		self.data = copy.deepcopy(exp)
		self.T = T
		self.origin_db = copy.deepcopy(self.data.db)
		self.db = self.data.db.__class__(**db_info)
		self.data.db = self.db
		self.xp_uuid = self.data.uuid
		self.graph_cfg = graph_cfg
		if 'tmax' not in graph_cfg:
			self.graph_cfg['tmax'] = self.data['exp'].-T[-1]
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		self.save()
		self.data = None

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		graph_cfg['tmin'] = self.graph_cfg['tmin']-0.1 - self.data['exp']._time_step
		graph_cfg['tmax'] = self.graph_cfg['tmin']-0.1
		while graph_cfg['tmax']<self.graph_cfg['tmax']:
			graph_cfg['tmax'] += self.data['exp']._time_step
			graph_cfg['tmin'] += self.data['exp']._time_step
			self.data.graph(autocommit=False, **graph_cfg)
			self.check_time()

	def get_data(self):
		self.db.get_experiment(uuid=self.xp_uuid)

	def save_data(self):
		self.data.commit_to_db()
		self.data.commit_data_to_db(self.data['graph'], self.graph_cfg['method'])

	def unpack_data(self):
		if hasattr(self.data.db, 'dbpath'):
			self.data.db.dbpath = os.path.join(self.path, self.data.db.dbpath)
		self.origin_db.merge(other_db=self.data.db, id_list=[self.xp_uuid], main_only=False)
		self.data.db = self.origin_db
		self.data.commit_to_db()


#id list is list
# other db instead of other db path
#save data graph???

