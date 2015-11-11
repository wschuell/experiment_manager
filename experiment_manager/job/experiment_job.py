
from. import Job

import time
import os
import shutil
import cPickle
import copy

class ExperimentJob(Job):

	def __init__(self, exp, tmax, *args,**kwargs):
		super(ExperimentJob, self).__init__(*args,**kwargs)
		self.data = copy.deepcopy(exp)
		self.xp_uuid = self.data.uuid
		self.tmax = tmax
		self.save()
		self.data = None

	def script(self):
		 while self.data._T[-1]<self.tmax:
			self.data.continue_exp()
			self.check_time()

	def get_data(self):
		with open(self.xp_uuid+'.b','r') as f:
			self.data = cPickle.loads(f.read())

	def save_data(self):
		with open(self.data.uuid+'.b','w') as f:
			f.write(cPickle.dumps(self.data))

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data.uuid+'.b', self.data.uuid+'_'+self.uuid+'.b')

	def __eq__(self, other):
		return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid

	def __lt__(self, other):
		return self.__eq__(other) and self.tmax < other.tmax

	def __ge__(self, other):
		return self.__eq__(other) and self.tmax >= other.tmax

class ExperimentDBJob(Job):

	def __init__(self, exp, tmax, db_cfg={}, **kwargs):
		super(ExperimentDBJob, self).__init__(**kwargs)
		self.data = copy.deepcopy(exp)
		self.tmax = tmax
		self.origin_db = copy.deepcopy(self.data.db)
		os.chdir(self.get_path())
		self.db = self.data.db.__class__(**db_cfg)
		os.chdir(self.get_back_path())
		self.data.db = self.db
		self.xp_uuid = self.data.uuid
		self.save()
		self.data = None

	def script(self):
		 while self.data._T[-1]<self.tmax:
			self.data.continue_exp(autocommit=False)
			self.check_time()

	def get_data(self):
		self.data = self.db.get_experiment(uuid=self.xp_uuid)

	def save_data(self):
		self.data.commit_to_db()

	def unpack_data(self):
		self.data.db = self.origin_db
		self.data.commit_to_db()

	def __eq__(self, other):
		return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid

	def __lt__(self, other):
		return self.__eq__(other) and self.tmax < other.tmax

	def __gt__(self, other):
		return self.__eq__(other) and self.tmax > other.tmax

	def __le__(self, other):
		return self.__eq__(other) and self.tmax <= other.tmax

	def __ge__(self, other):
		return self.__eq__(other) and self.tmax >= other.tmax


class GraphExpJob(ExperimentJob):

	def __init__(self, exp, graph_cfg, **kwargs):
		super(ExperimentJob, self).__init__(**kwargs)
		self.data = {}
		self.xp_uuid = exp.uuid
		self.graph_filename = None
		self.data['exp'] = copy.deepcopy(exp)
		self.graph_cfg = graph_cfg
		if 'tmax' not in graph_cfg:
			self.graph_cfg['tmax'] = self.data['exp']._T[-1]
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		self.save()
		self.data = None

	def __eq__(self, other):
		return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid

	def __lt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] >= other.graph_cfg['tmax']

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
			if self.data is None:
				self.data = {}
			self.data['exp'] = cPickle.loads(f.read())
		if self.graph_filename is not None and os.path.isfile(self.graph_filename+'.b'):
			with open(self.graph_filename+'.b', 'r') as f:
				self.data['graph'] = cPickle.loads(f.read())

	def save_data(self):
		with open(self.data['exp'].uuid+'.b','w') as f:
			f.write(cPickle.dumps(self.data['exp']))
		if 'graph' in self.data.keys():
			self.data['graph'].write_files()

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data['exp'].uuid+'.b', self.data['exp'].uuid+'_'+self.uuid+'.b')
		shutil.move(self.path+'/'+self.graph_filename+'.b', self.data['exp'].uuid+'_'+self.uuid+'.b')


class GraphExpDBJob(ExperimentDBJob):

	def __init__(self, exp, db_cfg={}, descr='', requirements=[], virtual_env=None, **graph_cfg):
		super(ExperimentDBJob, self).__init__(descr=descr, requirements=requirements, virtual_env=virtual_env)
		self.data = {}
		self.data['exp'] = copy.deepcopy(exp)
		self.xp_uuid = self.data['exp'].uuid
		self.db_cfg = db_cfg
		self.origin_db = copy.deepcopy(self.data['exp'].db)
		os.chdir(self.get_path())
		new_db = self.data['exp'].db.__class__(**self.db_cfg)
		os.chdir(self.get_back_path())
		self.db = new_db
		self.data['exp'].db = self.db
		self.graph_cfg = graph_cfg
		if 'tmax' not in graph_cfg:
			self.graph_cfg['tmax'] = self.data['exp']._T[-1]
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		if self.data['exp']._T[-1]<self.graph_cfg['tmax']:
			self.status = 'dependencies not satisfied'
		self.save()
		self.data = None

	def __eq__(self, other):
		try:
			return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid and self.graph_cfg['method'] == other.graph_cfg['method']
		except KeyError:
			return True

	def __lt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] > other.graph_cfg['tmax']

	def __le__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] >= other.graph_cfg['tmax']

	def __gt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] < other.graph_cfg['tmax']

	def __ge__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] <= other.graph_cfg['tmax']

	def re_init(self):
		self.data = {}
		self.data['exp'] = self.origin_db.get_experiment(uuid=self.xp_uuid)
		if self.data['exp'] is not None and self.data['exp']._T[-1] >= self.graph_cfg['tmax']:
			self.status = 'pending'
		self.data = None

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		if 'graph' in self.data.keys():
			tmax = self.data['graph']._X[0][-1] + self.data['exp']._time_step
		else:
			tmax = 0
		graph_cfg['tmax'] = max(tmax, self.graph_cfg['tmin']) + self.data['exp']._time_step -0.1
		graph_cfg['tmin'] = graph_cfg['tmax']- self.data['exp']._time_step
		while graph_cfg['tmax']<self.graph_cfg['tmax']:
			if 'graph' not in self.data.keys():
				self.data['graph'] = self.data['exp'].graph(autocommit=False, **graph_cfg)
				self.graph_filename = self.data['graph'].filename
			else:
				self.data['graph'].complete_with(self.data['exp'].graph(autocommit=False, **graph_cfg))
			self.check_time()
			graph_cfg['tmax'] += self.data['exp']._time_step
			graph_cfg['tmin'] += self.data['exp']._time_step

	def get_data(self):
		self.data = {}
		self.data['exp'] = self.db.get_experiment(uuid=self.xp_uuid)
		if self.db.data_exists(uuid=self.xp_uuid, method=self.graph_cfg['method']):
			self.data['graph'] = self.db.get_graph(uuid=self.xp_uuid,method=self.graph_cfg['method'])
			self.graph_filename = self.data['graph'].filename

	def save_data(self):
		self.data['exp'].commit_to_db()
		if 'graph' in self.data.keys():
			self.data['exp'].commit_data_to_db(self.data['graph'], self.graph_cfg['method'])

	def unpack_data(self):
		if hasattr(self.data['exp'].db, 'dbpath'):
			self.data['exp'].db.dbpath = os.path.join(self.path, self.data['exp'].db.dbpath)
		self.origin_db.merge(other_db=self.data['exp'].db, id_list=[self.xp_uuid], main_only=False)
		self.data['exp'].db = self.origin_db
		self.data['exp'].commit_to_db()

	def gen_depend(self):
		exp = self.origin_db.get_experiment(uuid=self.xp_uuid)
		tmax = self.graph_cfg['tmax']
		return [ExperimentDBJob(tmax=tmax, exp=exp, db_cfg=self.db_cfg, descr='dependency_of_'+self.descr, requirements=self.requirements, virtual_env=self.virtual_env)]


#id list is list
#save data graph???

