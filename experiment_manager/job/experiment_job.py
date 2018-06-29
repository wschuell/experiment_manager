
from. import Job

import time
import os
import shutil
try:
	import cPickle as pickle
except ImportError:
	import pickle
import copy
import path
import errno
import math
import path as pathpy
import naminggamesal as ngal

class ExperimentJob(Job):

	def __init__(self, exp, tmax, *args,**kwargs):
		super(ExperimentJob, self).__init__(*args,**kwargs)
		if exp._T[-1] >= tmax:
			self.status = 'already done'
		self.data = exp #copy.deepcopy(exp)
		self.xp_uuid = self.data.uuid
		self.tmax = tmax
		self.files.append(self.xp_uuid+'.b')
		self.save(keep_data=False)
		self.close_connections()

	def script(self):
		while self.data._T[-1]<self.tmax:
			self.data.continue_exp()
			self.check_time()

	def get_data(self):
		with open(self.xp_uuid+'.b','rb') as f:
			self.data = pickle.loads(f.read())

	def save_data(self):
		with open(self.data.uuid+'.b','wb') as f:
			f.write(pickle.dumps(self.data))

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data.uuid+'.b', self.data.uuid+'_'+self.uuid+'.b')

	def __eq__(self, other):
		return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid

	def __lt__(self, other):
		return self.__eq__(other) and self.tmax < other.tmax

	def __ge__(self, other):
		return self.__eq__(other) and self.tmax >= other.tmax


class ExperimentDBJob(Job):

	def __init__(self, tmax, exp=None, xp_uuid=None, db=None, db_cfg={}, profiling=False, checktime=True, estimated_time=2*3600, **kwargs):
		self.tmax = tmax
		if exp is None:
			xp_tmax = db.get_param(xp_uuid=xp_uuid,param='Tmax')
		else:
			xp_tmax = exp._T[-1]
		if xp_tmax >= tmax:
			raise Exception('Job already done')
			#self.status = 'already done'
			#self.xp_uuid = None
		#else:
		super(ExperimentDBJob, self).__init__(get_data_at_unpack=False,profiling=profiling, checktime=checktime, estimated_time=estimated_time, **kwargs)
		if exp is None:
			self.origin_db = db
			with path.Path(self.get_path()):
				self.db = db.__class__(db_type="sqlite3",**db_cfg)
			self.xp_uuid = xp_uuid
		else:
			self.data = {'exp':exp} #copy.deepcopy(exp)
			self.origin_db = self.data['exp'].db #copy.deepcopy(self.data.db)
			with path.Path(self.get_path()):
				self.db = self.data['exp'].db.__class__(db_type="sqlite3",**db_cfg)
			self.data['exp'].db = self.db
			self.xp_uuid = self.data['exp'].uuid
		#db_path = self.db.dbpath
		if os.path.isfile(self.db.dbpath):
			db_path = self.db.dbpath
		else:
			db_path = os.path.basename(self.db.dbpath)
		self.files.append(db_path)
		#self.db.dbpath = os.path.join(self.get_path(),self.db.dbpath)
		if hasattr(self,'methods'):
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid],methods=self.methods)
		else:
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid])
		#self.db.dbpath = db_path
		source_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		dst_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		#try:
		#	os.makedirs(os.path.join(self.get_path(),'data/'))
		#except OSError as exc:  # Python >2.5
		#	if exc.errno == errno.EEXIST and os.path.isdir(os.path.join(self.get_path(),'data/')):
		#		pass
		#	else:
		#		raise
		if os.path.exists(source_file):
			if len(os.path.dirname(dst_file))>0 and not os.path.exists(os.path.dirname(dst_file)):
				os.makedirs(os.path.dirname(dst_file))
			shutil.copy(source_file, dst_file)
		self.files.append('data/'+self.xp_uuid+'.db.xz')
		self.clean_at_retrieval = ['data/'+self.xp_uuid+'.db','data/'+self.xp_uuid+'.db-journal']

		self.save(keep_data=False)
		self.db.close()

	def script(self):
		self.data['exp'].continue_exp_until(T=self.tmax,autocommit=False,monitoring_func=self.monitoring_func)

	def monitoring_func(self,*args,**kwargs):
		self.check_time()

	def get_data(self):
		if not hasattr(self.db,'connection'):
			try:
				self.db.reconnect()
			except AttributeError:
				raise AttributeError(str(ngal.ngdb.NamingGamesDB.instances))
		self.data = dict()
		self.data['exp'] = self.db.get_experiment(xp_uuid=self.xp_uuid)

	def save_data(self):
		self.data['exp'].compress(rm=False)
		self.db.commit(self.data['exp'])

	def unpack_data(self):
		#if os.path.isfile(os.path.join(self.path, self.db.dbpath)):
		#	self.db.dbpath = os.path.join(self.path, self.db.dbpath)
		with pathpy.Path(self.get_path()):
			if not hasattr(self.db,'connection'):
				self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			raise IOError('global database not active')
			self.origin_db.reconnect()#RAM_only=True)
		if hasattr(self,'methods'):
			self.db.export(other_db=self.origin_db, id_list=[self.xp_uuid],methods=self.methods)
		else:
			self.db.export(other_db=self.origin_db, id_list=[self.xp_uuid])
		source_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		dst_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		if os.path.exists(source_file):
			if len(os.path.dirname(dst_file))>0 and not os.path.exists(os.path.dirname(dst_file)):
				os.makedirs(os.path.dirname(dst_file))
			shutil.copy(source_file, dst_file)
		self.db.close()
		self.origin_db.close()
		#self.data.db = self.origin_db
		#self.data.commit_to_db()


	def close_connections(self):
		self.db.close()
		self.origin_db.close()

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

	def fix(self):
		Job.fix(self)
		if self.db.dbpath in self.files:
			self.files.remove(self.db.dbpath)
	def re_init(self):
		with pathpy.Path(self.get_path()):
			if not hasattr(self.db,'connection'):
				self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			self.origin_db.reconnect()#RAM_only=True)
		if hasattr(self,'methods'):
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid],methods=self.methods)
		else:
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid])
		#self.db.dbpath = db_path
		source_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		dst_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		if os.path.exists(source_file):
			try:
				os.makedirs(os.path.join(self.get_path(),'data/'))
			except OSError as exc:  # Python >2.5
				if exc.errno == errno.EEXIST and os.path.isdir(os.path.join(self.get_path(),'data/')):
					pass
				else:
					raise
			shutil.copy(source_file, dst_file)
		elif os.path.exists(dst_file):
			os.remove(dst_file)
		self.close_connections()

	def restart(self):
		self.re_init()

class GraphExpJob(ExperimentJob):

	def __init__(self, exp, graph_cfg, **kwargs):
		super(ExperimentJob, self).__init__(**kwargs)
		self.data = {}
		self.graph_filename = None
		self.xp_uuid = exp.uuid
		self.data['exp'] = exp #copy.deepcopy(exp)
		self.graph_cfg = graph_cfg
		self.graph_cfg['tmax'] = max(xp_tmax,self.graph_cfg['tmax'])
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		self.save(keep_data=False)

	def __eq__(self, other):
		return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid

	def __lt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] >= other.graph_cfg['tmax']

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		graph_cfg['tmin'] = self.graph_cfg['tmin']-0.1 - self.data['exp'].stepfun(self.graph_cfg['tmin'],backwards=True)
		graph_cfg['tmax'] = self.graph_cfg['tmin']-0.1
		while graph_cfg['tmax']<self.graph_cfg['tmax']:
			#graph_cfg['tmax'] += self.data['exp']._time_step
			#graph_cfg['tmin'] += self.data['exp']._time_step
			graph_cfg['tmax'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmax']))
			graph_cfg['tmin'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
			if 'graph' not in list(self.data.keys()):
				self.data['graph'] = self.data['exp'].graph(**graph_cfg)
				self.graph_filename = self.data['graph'].filename
			else:
				cfunc = getattr(ngal.ngmeth,'custom_'+graph_cfg['method'])
				if cfunc.level != 'exp':
					self.data['graph'].complete_with(self.data['exp'].graph(**graph_cfg))
				else:
					self.data['graph'] = self.data['exp'].graph(**graph_cfg)
			self.check_time()

	def get_data(self):
		with open(self.xp_uuid+'.b','rb') as f:
			if self.data is None:
				self.data = {}
			self.data['exp'] = pickle.loads(f.read())
		if self.graph_filename is not None and os.path.isfile(self.graph_filename+'.b'):
			with open(self.graph_filename+'.b', 'rb') as f:
				self.data['graph'] = pickle.loads(f.read())

	def save_data(self):
		with open(self.data['exp'].uuid+'.b','wb') as f:
			f.write(pickle.dumps(self.data['exp']))
		if 'graph' in list(self.data.keys()):
			self.data['graph'].write_files()

	def unpack_data(self):
		shutil.move(self.path+'/'+self.data['exp'].uuid+'.b', self.data['exp'].uuid+'_'+self.uuid+'.b')
		shutil.move(self.path+'/'+self.graph_filename+'.b', self.data['exp'].uuid+'_'+self.uuid+'.b')


class GraphExpDBJob(ExperimentDBJob):
	def __init__(self, xp_uuid=None, db=None, exp=None, db_cfg={}, descr=None, requirements=[], virtual_env=None, profiling=False, checktime=True, estimated_time=3600, **graph_cfg):
		super(ExperimentDBJob, self).__init__(descr=descr, requirements=requirements, virtual_env=virtual_env,profiling=profiling, checktime=checktime, estimated_time=estimated_time)
		try:
			if exp is None:
				tmax_db = db.get_param(xp_uuid=xp_uuid, method=graph_cfg['method'], param='Time_max')
			else:
				tmax_db = exp.db.get_param(xp_uuid=exp.uuid, method=graph_cfg['method'], param='Time_max')
		except TypeError:
			tmax_db = -1
		if tmax_db >= graph_cfg['tmax']:
			self.status = 'already done'
		else:
			self.data = {}
			self.graph_cfg = graph_cfg
			self.db_cfg = db_cfg
			if exp is not None:
				self.xp_uuid = exp.uuid
				self.origin_db = exp.db
				xp_tmax = exp._T[-1]
			else:
				self.xp_uuid = xp_uuid
				self.origin_db = db
				try:
					xp_tmax = self.origin_db.get_param(xp_uuid=self.xp_uuid, param='Time_max',method=graph_cfg['method'])
				except TypeError:
					xp_tmax = -1
			self.graph_cfg['tmax'] = max(xp_tmax,self.graph_cfg['tmax'])
			if 'tmin' not in graph_cfg:
				self.graph_cfg['tmin'] = 0
			if xp_tmax<self.graph_cfg['tmax']:
				self.status = 'dependencies not satisfied'
			with path.Path(self.get_path()):
				new_db = self.origin_db.__class__(db_type="sqlite3",**self.db_cfg)
			self.db = new_db
			#db_path = self.db.dbpath
			#self.db.dbpath = os.path.join(self.get_path(),self.db.dbpath)
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid], methods=[graph_cfg['method']])
			#self.db.dbpath = db_path
			if os.path.isfile(self.db.dbpath):
				db_path = self.db.dbpath
			else:
				db_path = os.path.basename(self.db.dbpath)
			self.files.append(db_path)
			source_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
			dst_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
			try:
				os.makedirs(os.path.join(self.get_path(),'data/'))
			except OSError as exc:  # Python >2.5
				if exc.errno == errno.EEXIST and os.path.isdir(os.path.join(self.get_path(),'data/')):
					pass
				else:
					raise
			shutil.copy(source_file, dst_file)
			self.files.append('data/'+self.xp_uuid+'.db.xz')
			self.clean_at_retrieval = ['data/'+self.xp_uuid+'.db','data/'+self.xp_uuid+'.db-journal']
		self.save(keep_data=False)
		self.close_connections()

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
		#self.db.dbpath = os.path.join(self.path, self.db.dbpath)
		with pathpy.Path(self.get_path()):
			if not hasattr(self.db,'connection'):
				self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			self.origin_db.reconnect()#RAM_only=True)
		self.data = {}
		#self.data['exp'] = self.origin_db.get_experiment(xp_uuid=self.xp_uuid)
		#if self.data['exp'] is not None and self.data['exp']._T[-1] >= self.graph_cfg['tmax']:
		#old_path = self.db.dbpath
		if self.origin_db.id_in_db(xp_uuid=self.xp_uuid):
			T = self.origin_db.get_param(xp_uuid=self.xp_uuid,param='Tmax')
			if T >= self.graph_cfg['tmax']:
				self.graph_cfg['tmax'] = T # To avoid being between two snapshot step values
				if not (self.db.id_in_db(xp_uuid=self.xp_uuid) and self.db.get_param(xp_uuid=self.xp_uuid,param='Tmax')>=T):
					self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid])
				self.status = 'pending'
		#self.db.dbpath = old_path
		self.save(keep_data=False)
		self.origin_db.close()
		self.db.close()

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		if 'graph' in list(self.data.keys()):
			tmax = self.data['graph']._X[0][-1]
		else:
			tmax = -self.data['exp'].stepfun(0,backwards=True)
		graph_cfg['tmin'] = max(tmax + self.data['exp'].stepfun(tmax), self.graph_cfg['tmin']) - 0.1
		graph_cfg['tmax'] = graph_cfg['tmin'] + self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
		while graph_cfg['tmax']<self.graph_cfg['tmax'] + self.data['exp'].stepfun(self.graph_cfg['tmax']):
			if 'graph' not in list(self.data.keys()):
				self.data['graph'] = self.data['exp'].graph(autocommit=False, **graph_cfg)
				self.graph_filename = self.data['graph'].filename
			else:
				cfunc = getattr(ngal.ngmeth,'custom_'+graph_cfg['method'])
				if cfunc.level != 'exp':
					self.data['graph'].complete_with(self.data['exp'].graph(autocommit=False, **graph_cfg), remove_duplicates=True)
				else:
					self.data['graph'] = self.data['exp'].graph(autocommit=False, **graph_cfg)
			graph_cfg['tmax'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmax']))
			graph_cfg['tmin'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
			self.check_time()

	def get_data(self):
		if not hasattr(self.db,'connection'):
			self.db.reconnect()
		self.data = {}
		self.data['exp'] = self.db.get_experiment(xp_uuid=self.xp_uuid)
		if self.db.data_exists(xp_uuid=self.xp_uuid, method=self.graph_cfg['method']):
			self.data['graph'] = self.db.get_graph(xp_uuid=self.xp_uuid,method=self.graph_cfg['method'])
			self.graph_filename = self.data['graph'].filename

	def save_data(self):
		if 'exp' in list(self.data.keys()) and not (self.db.id_in_db(xp_uuid=self.xp_uuid) and self.db.get_param(xp_uuid=self.xp_uuid,param='Tmax')>=self.data['exp']._T[-1]):
			self.db.commit(self.data['exp'])
		#elif not self.injobdir:
		#	self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid])
		#else:
		if 'graph' in list(self.data.keys()):
			self.data['exp'].commit_data_to_db(self.data['graph'], self.graph_cfg['method'])

	def unpack_data(self):
		with pathpy.Path(self.get_path()):
			if not hasattr(self.db,'connection'):
				self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			raise IOError('global database not active')
			self.origin_db.reconnect()#RAM_only=True)
		self.db.export(other_db=self.origin_db, id_list=[self.xp_uuid], methods=[self.graph_cfg['method']], graph_only=True)
		#source_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		#dst_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		#shutil.copy(source_file, dst_file)
		#if hasattr(self.data['exp'].db, 'dbpath'):
		#	self.data['exp'].db.dbpath = os.path.join(self.path, self.data['exp'].db.dbpath)
		#self.origin_db.merge(other_db=self.data['exp'].db, id_list=[self.xp_uuid], main_only=False)
		#self.data['exp'].db = self.origin_db
		#self.data['exp'].commit_to_db()
		self.origin_db.close()
		self.db.close()

	def gen_depend(self):
		#exp = self.origin_db.get_experiment(uuid=self.xp_uuid)
		tmax = self.graph_cfg['tmax']
		return [ExperimentDBJob(tmax=tmax,  estimated_time=self.estimated_time, profiling=self.profiling, checktime=self.checktime, xp_uuid=self.xp_uuid, db=self.origin_db, db_cfg=self.db_cfg, descr=None, requirements=self.requirements, virtual_env=self.virtual_env)]


#id list is list
#save data graph???


class MultipleGraphExpDBJob(ExperimentDBJob):

	def __init__(self, xp_uuid=None, db=None, exp=None, db_cfg={}, descr=None, requirements=[], virtual_env=None, profiling=False, checktime=True, estimated_time=2*3600, **graph_cfg):
		self.dep_path = None
		methods = graph_cfg['method']
		if not isinstance(methods, list):
			methods = [methods]
		self.methods = methods
		tmax_db = graph_cfg['tmax']
		for mt in self.methods:
			try:
				if exp is None:
					tmax_db = min(tmax_db,db.get_param(xp_uuid=xp_uuid, method=mt, param='Time_max'))
				else:
					tmax_db = min(tmax_db,exp.db.get_param(xp_uuid=exp.uuid, method=mt, param='Time_max'))
			except TypeError:#when the entry doesnt exist in the database (iow Tmax is 0)
				tmax_db = -1
		if tmax_db >= graph_cfg['tmax']:
			raise Exception('Job already done')
		else:
			super(ExperimentDBJob, self).__init__(descr=descr, requirements=requirements, virtual_env=virtual_env, profiling=profiling, get_data_at_unpack=False,checktime=checktime, estimated_time=estimated_time)
			self.data = {}
			self.graph_cfg = graph_cfg
			self.db_cfg = db_cfg
			if exp is not None:
				self.xp_uuid = exp.uuid
				self.origin_db = exp.db
				xp_tmax = exp._T[-1]
			else:
				self.xp_uuid = xp_uuid
				self.origin_db = db
				try:
					xp_tmax = self.origin_db.get_param(xp_uuid=self.xp_uuid, param='Tmax')
				except TypeError:
					xp_tmax = -1
			self.graph_cfg['tmax'] = max(xp_tmax,self.graph_cfg['tmax'])
			if 'tmin' not in graph_cfg:
				self.graph_cfg['tmin'] = 0
			if xp_tmax<self.graph_cfg['tmax']:
				self.status = 'dependencies not satisfied'
			with path.Path(self.get_path()):
				new_db = self.origin_db.__class__(db_type="sqlite3",**self.db_cfg)
			self.db = new_db
			#db_path = self.db.dbpath
			#self.db.dbpath = os.path.join(self.get_path(),self.db.dbpath)
			self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid], methods=self.methods)
			#self.db.dbpath = db_path
			if os.path.isfile(self.db.dbpath):
				db_path = self.db.dbpath
			else:
				db_path = os.path.basename(self.db.dbpath)
			self.files.append(db_path)
			try:
				os.makedirs(os.path.join(self.get_path(),'data/'))
			except OSError as exc:  # Python >2.5
				if exc.errno == errno.EEXIST and os.path.isdir(os.path.join(self.get_path(),'data/')):
					pass
				else:
					raise
			if not self.status == 'dependencies not satisfied':
				source_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
				dst_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
				shutil.copy(source_file, dst_file)
				self.files.append('data/'+self.xp_uuid+'.db.xz')
			self.clean_at_retrieval = ['data/'+self.xp_uuid+'.db','data/'+self.xp_uuid+'.db-journal']
		self.save(keep_data=False)
		self.close_connections()



	def __eq__(self, other):
		try:
			return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid and set(self.methods) & set(other.methods)
		except KeyError:
			return True

	def __lt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] > other.graph_cfg['tmax']

	#def __le__(self, other):
	#	return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] >= other.graph_cfg['tmax']

	#def __gt__(self, other):
	#	return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] < other.graph_cfg['tmax']

	def __ge__(self, other):
		return (self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] <= other.graph_cfg['tmax']) and set(other.methods) <= set(self.methods)

	def re_init(self):
		#self.db.dbpath = os.path.join(self.path, self.db.dbpath)
		with pathpy.Path(self.get_path()):
			if not hasattr(self.db,'connection'):
				self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			self.origin_db.reconnect()#RAM_only=True)
		self.data = {}
		#self.data['exp'] = self.origin_db.get_experiment(xp_uuid=self.xp_uuid)
		#if self.data['exp'] is not None and self.data['exp']._T[-1] >= self.graph_cfg['tmax']:
		#old_path = self.db.dbpath
		if self.origin_db.id_in_db(xp_uuid=self.xp_uuid):
			T = self.origin_db.get_param(xp_uuid=self.xp_uuid,param='Tmax')
			if T >= self.graph_cfg['tmax']:
				self.graph_cfg['tmax'] = T # To avoid being between two snapshot step values
				#if not (self.db.id_in_db(xp_uuid=self.xp_uuid) and self.db.get_param(xp_uuid=self.xp_uuid,param='Tmax')>=T):
				#if self.dep_path is None:
				self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid],methods=self.methods)#new policy: always get from origin_db and not dep_path
				self.status = 'pending'
		#self.db.dbpath = old_path

		source_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		dst_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		shutil.copy(source_file, dst_file)
		self.files.append('data/'+self.xp_uuid+'.db.xz')

		self.save(keep_data=False)
		self.origin_db.close()
		self.db.close()

	def restart(self):
		self.re_init()

	def script(self):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		for method in self.methods:
			graph_cfg['method'] = method
			if method in list(self.data.keys()):
				tmax = self.data[method]._X[0][-1]
			else:
				tmax = -self.data['exp'].stepfun(0,backwards=True)
			graph_cfg['tmin'] = max(tmax + self.data['exp'].stepfun(tmax), self.graph_cfg['tmin']) - 0.1
			graph_cfg['tmax'] = graph_cfg['tmin'] + self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
			while graph_cfg['tmax']<self.graph_cfg['tmax'] + self.data['exp'].stepfun(self.graph_cfg['tmax']):
				if method not in list(self.data.keys()):
					self.data[method] = self.data['exp'].graph(autocommit=False, **graph_cfg)
					self.graph_filename = self.data[method].filename
				else:
					cfunc = getattr(ngal.ngmeth,'custom_'+graph_cfg['method'])
					if cfunc.level != 'exp':
						self.data[method].complete_with(self.data['exp'].graph(autocommit=False, **graph_cfg), remove_duplicates=True)
					else:
						self.data[method] = self.data['exp'].graph(autocommit=False, **graph_cfg)
				graph_cfg['tmax'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmax']))
				graph_cfg['tmin'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
				self.check_time()
			self.save_data()
		del self.data['exp']
		self.db.delete(id_list=[self.xp_uuid],xp_only=True)

	def get_data(self):
		if not hasattr(self.db,'connection'):
			self.db.reconnect()
		self.data = {}
		if self.dep_path and (not self.db.id_in_db(xp_uuid=self.xp_uuid) or int(self.db.get_param(param='Tmax', xp_uuid=self.xp_uuid))<self.graph_cfg['tmax']):
			dep_db = self.db.__class__(db_type="sqlite3",path=os.path.join(self.get_back_path(),self.dep_path,'naminggames.db'))
			dep_db.export(other_db=self.db, id_list=[self.xp_uuid], methods=self.methods)
			self.data['exp'] = self.db.get_experiment(xp_uuid=self.xp_uuid)
			source_file = os.path.join(self.get_back_path(),self.dep_path,'data',self.xp_uuid+'.db.xz')
			dst_file = os.path.join('data',self.xp_uuid+'.db.xz')#self.get_path(),'data',self.xp_uuid+'.db.xz')
			shutil.copy(source_file, dst_file)
			dep_db.close()

		#if not os.path.isfile(os.path.join('data',self.xp_uuid+'.db.xz')):#self.get_path(),'data',self.xp_uuid+'.db.xz')):
			#source_file = os.path.join(self.get_back_path(),self.dep_path,'data',self.xp_uuid+'.db.xz')
			#dst_file = os.path.join('data',self.xp_uuid+'.db.xz')#self.get_path(),'data',self.xp_uuid+'.db.xz')
			#shutil.copy(source_file, dst_file)
			#if 'data/'+self.xp_uuid+'.db.xz' not in self.files: #not needed, if file gotten from deps; will stay in place for later run of the same job;
			#	self.files.append('data/'+self.xp_uuid+'.db.xz')

		self.data['exp'] = self.db.get_experiment(xp_uuid=self.xp_uuid)
		for method in self.methods:
			if self.db.data_exists(xp_uuid=self.xp_uuid, method=method):
				self.data[method] = self.db.get_graph(xp_uuid=self.xp_uuid,method=method)
				self.graph_filename = self.data[method].filename

	def save_data(self):
		if 'exp' in list(self.data.keys()) and not (self.db.id_in_db(xp_uuid=self.xp_uuid) and self.db.get_param(xp_uuid=self.xp_uuid,param='Tmax')>=self.data['exp']._T[-1]):
			self.db.commit(self.data['exp'])
		#elif not self.injobdir:
		#	self.origin_db.export(other_db=self.db, id_list=[self.xp_uuid])
		#else:
		for method in self.methods:
			if method in list(self.data.keys()) and 'exp' in self.data.keys():
				self.data['exp'].commit_data_to_db(self.data[method], method)

	def unpack_data(self):
		if os.path.isfile(os.path.join(self.path, self.db.dbpath)):
			self.db.dbpath = os.path.join(self.path, self.db.dbpath)
		if not hasattr(self.db,'connection'):
			self.db.reconnect()
		if not hasattr(self.origin_db,'connection'):
			raise IOError('global database not active')
			self.origin_db.reconnect()#RAM_only=True)
		self.db.export(other_db=self.origin_db, id_list=[self.xp_uuid], methods=self.methods, graph_only=True)
		#source_file = os.path.join(self.get_path(),'data',self.xp_uuid+'.db.xz')
		#dst_file = os.path.join(os.path.dirname(self.origin_db.dbpath),'data',self.xp_uuid+'.db.xz')
		#shutil.copy(source_file, dst_file)
		#if hasattr(self.data['exp'].db, 'dbpath'):
		#	self.data['exp'].db.dbpath = os.path.join(self.path, self.data['exp'].db.dbpath)
		#self.origin_db.merge(other_db=self.data['exp'].db, id_list=[self.xp_uuid], main_only=False)
		#self.data['exp'].db = self.origin_db
		#self.data['exp'].commit_to_db()
		self.origin_db.close()
		self.db.close()

	def gen_depend(self):
		#exp = self.origin_db.get_experiment(xp_uuid=self.xp_uuid)
		tmax = self.graph_cfg['tmax']
		j = ExperimentDBJob(tmax=tmax, estimated_time=self.estimated_time, profiling=self.profiling, checktime=self.checktime, xp_uuid=self.xp_uuid, db=self.origin_db, db_cfg=self.db_cfg, descr=None, requirements=self.requirements, virtual_env=self.virtual_env)
		self.dep_path = j.path
		return[j]

	def fix(self):
		Job.fix(self)
		if self.db.dbpath in self.files:
			self.files.remove(self.db.dbpath)


#id list is list
#save data graph???













class ExperimentDBJobNoStorage(ExperimentDBJob):

	def __init__(self, tmax, exp=None, xp_uuid=None, db=None, db_cfg={},descr=None, requirements=[], virtual_env=None, profiling=False, checktime=True, estimated_time=2*3600, **graph_cfg):
		methods = graph_cfg['method']
		self.graph_cfg = graph_cfg
		self.graph_cfg['tmax'] = tmax
		if 'tmin' not in graph_cfg:
			self.graph_cfg['tmin'] = 0
		if not isinstance(methods, list):
			methods = [methods]
		self.methods = methods
		ExperimentDBJob.__init__(self,tmax=tmax,db=db,db_cfg=db_cfg,xp_uuid=xp_uuid,exp=exp,profiling=profiling, checktime=checktime, estimated_time=estimated_time, descr=descr, requirements=requirements, virtual_env=virtual_env)


	def monitoring_func(self,*args,**kwargs):
		graph_cfg = copy.deepcopy(self.graph_cfg)
		for method in self.methods:
			graph_cfg['method'] = method
			if method in list(self.data.keys()):
				tmax = self.data[method]._X[0][-1]
			else:
				tmax = -self.data['exp'].stepfun(0,backwards=True)
			graph_cfg['tmin'] = max(tmax + self.data['exp'].stepfun(tmax), self.graph_cfg['tmin']) - 0.1
			graph_cfg['tmax'] = graph_cfg['tmin'] + self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
			while graph_cfg['tmax']<self.graph_cfg['tmax'] + self.data['exp'].stepfun(self.graph_cfg['tmax']):
				if method not in list(self.data.keys()):
					self.data[method] = self.data['exp'].graph(autocommit=False, **graph_cfg)
					self.graph_filename = self.data[method].filename
				else:
					cfunc = getattr(ngal.ngmeth,'custom_'+graph_cfg['method'])
					if cfunc.level != 'exp':
						self.data[method].complete_with(self.data['exp'].graph(autocommit=False, **graph_cfg), remove_duplicates=True)
					else:
						self.data[method] = self.data['exp'].graph(autocommit=False, **graph_cfg)
				graph_cfg['tmax'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmax']))
				graph_cfg['tmin'] += self.data['exp'].stepfun(math.ceil(graph_cfg['tmin']))
		self.check_time()

	def get_data(self):
		ExperimentDBJob.get_data(self)
		self.data['exp'].no_storage = True
		for method in self.methods:
			if self.db.data_exists(xp_uuid=self.xp_uuid, method=method):
				self.data[method] = self.db.get_graph(xp_uuid=self.xp_uuid,method=method)
				self.graph_filename = self.data[method].filename

	def save_data(self):
		self.db.commit(self.data['exp'])
		for method in self.methods:
			if method in list(self.data.keys()) and 'exp' in self.data.keys():
				self.data['exp'].commit_data_to_db(self.data[method], method)
		self.data.compress(rm=False)
		self.db.commit(self.data)


	def __eq__(self, other):
		try:
			return self.__class__ == other.__class__ and self.xp_uuid == other.xp_uuid and set(self.methods) & set(other.methods)
		except KeyError:
			return True

	def __lt__(self, other):
		return self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] > other.graph_cfg['tmax']

	def __ge__(self, other):
		return (self.__eq__(other) and self.graph_cfg['tmax'] < other.graph_cfg['tmax'] and self.graph_cfg['tmin'] <= other.graph_cfg['tmax']) and set(other.methods) <= set(self.methods)
