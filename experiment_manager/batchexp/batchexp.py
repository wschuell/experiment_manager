import uuid
import copy
import json
from ..job_queue import get_jobqueue
#from ..database import get_database
from ..job.experiment_job import ExperimentDBJob, GraphExpDBJob, MultipleGraphExpDBJob


class BatchExp(object):

	def __init__(self, name=None, jq_cfg = None, estimated_time=1800, db_cfg=None, db=None, other_dbs=[], other_dbs_lookup=True, auto_job=True, profiling=False, virtual_env=None, requirements=[], **kwargs):
		self.uuid = str(uuid.uuid1())
		if name is None:
			self.name = self.uuid
		else:
			self.name = name
		if jq_cfg == None:
			self.jq_cfg = {'jq_type':'local'}
		else:
			self.jq_cfg = jq_cfg
		if 'name' not in list(self.jq_cfg.keys()):
			self.jq_cfg['name'] = self.name
		self.jobqueue = get_jobqueue(**self.jq_cfg)
		miss_list = [j for j in self.jobqueue.job_list if j.status in ['missubmitted','script error']]
		if len(miss_list) == len(self.jobqueue.job_list):
			self.jobqueue.reinit_missubmitted()
		if db is not None:
			self.db = db
		elif db_cfg is not None:
			self.db = get_database(**db_cfg)
		else:
			self.db = get_database(**{'db_type':'sqlite','name':self.name})
		self.other_dbs = other_dbs
		self.other_dbs_lookup = other_dbs_lookup
		self.auto_job = auto_job
		self.virtual_env = virtual_env
		self.requirements = requirements
		self.profiling = profiling
		self.estimated_time = estimated_time
		if not hasattr(self.jobqueue,'past_job_cfg'):
			self.jobqueue.past_job_cfg = []
#	def control_exp(self, exp):
#		exp.originclass = copy.deepcopy(exp.__class__)
#		exp.__class__ = Experiment
#		exp._batch_exp = self
#
#	def uncontrol_exp(self, exp):
#		exp.__class__ = exp.originclass
#		delattr(exp,'_batch_exp')
#		delattr(exp,'originclass')

	def get_experiment(self, xp_uuid=None, force_new=False, blacklist=[], pattern=None, tmax=0, auto_job=True, **xp_cfg):
		exp = self.db.get_experiment(xp_uuid=xp_uuid, force_new=force_new, blacklist=blacklist, pattern=pattern, tmax=tmax, **xp_cfg)
#		self.control_exp(exp)
		if auto_job and exp._T[-1] < tmax:
			self.add_exp_job(xp_uuid=exp.uuid, tmax=tmax)
			print('added job for exp {}, from {} to {}'.format(xp_uuid, exp._T[-1], tmax))
		return exp

	def get_graph(self, xp_uuid, method, tmin=0, tmax=None):
		if self.db.data_exists(xp_uuid=xp_uuid, method=method):
			graph = self.db.get_graph(xp_uuid=xp_uuid, method=method)
			return graph
#		self.control_exp(exp)
		if self.auto_job and exp._T[-1] < tmax:
			self.add_graph_job(xp_uuid=exp.uuid, method=method, tmax=tmax)
			print('added graph job for exp {}, method {} to {}'.format(xp_uuid, method, tmax))

	def add_exp_job(self, tmax, xp_uuid=None, save=True, xp_cfg={}):
		exp = self.get_experiment(xp_uuid=xp_uuid, **xp_cfg)
		if exp._T[-1] < tmax:
			job = ExperimentDBJob(exp=exp, tmax=tmax, virtual_env=self.virtual_env, requirements=self.requirements, profiling=self.profiling, checktime=True, estimated_time=self.estimated_time)
			self.jobqueue.add_job(job,save=save)

	def add_graph_job(self, method, xp_uuid=None, tmax=None, save=True, xp_cfg={}):
		if xp_uuid is None:
			exp = self.get_experiment(**xp_cfg)#modify in order to get only uuid and not whole exp
			tmax_xp = exp._T[-1]
		else:
			exp = None
			tmax_xp = self.db.get_param(xp_uuid=xp_uuid,param='Tmax')
		if tmax is None:
			tmax = tmax_xp
		try:
			job = MultipleGraphExpDBJob(xp_uuid=xp_uuid, db=self.db, exp=exp, method=method, tmax=tmax, virtual_env=self.virtual_env, requirements=self.requirements, profiling=self.profiling, checktime=True, estimated_time=self.estimated_time)
			self.jobqueue.add_job(job,save=save)
		except Exception as e:
			if e.args[0] != 'Job already done':
				raise


	def add_jobs(self, cfg_list, save_jq=True):
		for cfg in cfg_list:
			cfg_str = json.dumps(cfg, sort_keys=True)
			if cfg_str not in self.jobqueue.past_job_cfg:
				self.jobqueue.past_job_cfg.append(cfg_str)
				if 'uuid' in list(cfg.keys()):
					nb_iter = 1
				elif 'nb_iter' not in list(cfg.keys()):
					nb_iter = 1
				else:
					nb_iter = cfg['nb_iter']
				uuid_l = []
				if 'uuid' not in list(cfg.keys()):
					uuid_l = self.db.get_id_list(**cfg['xp_cfg'])
					if nb_iter > len(uuid_l):
						for i in range(nb_iter-len(uuid_l)):
							exp = self.db.get_experiment(blacklist=uuid_l, **cfg['xp_cfg'])
							uuid1 = exp.uuid
							uuid_l.append(uuid1)
					else:
						uuid_l = uuid_l[:nb_iter]
				else:
					uuid_l = [cfg['uuid']]
				cfg2 = dict((k,cfg[k]) for k in ('method', 'tmax') if k in list(cfg.keys()))
				if 'method' in list(cfg.keys()):
					for xp_uuid in uuid_l:
						self.add_graph_job(xp_uuid=xp_uuid,save=False,**cfg2)
				else:
					for xp_uuid in uuid_l:
						self.add_exp_job(xp_uuid=xp_uuid,save=False,**cfg2)
		if save_jq:
			self.jobqueue.save()

	def update_queue(self):
		try:
			self.jobqueue.update_queue()
		finally:
			self.db.commit_from_RAM()
		self.db.commit_from_RAM()


	def auto_finish_queue(self,t=60,coeff=1.):
		try:
			self.jobqueue.auto_finish_queue(t=t,coeff=coeff,call_between=self.db.commit_from_RAM)
		finally:
			self.db.commit_from_RAM()
		self.db.commit_from_RAM()


#class Experiment(object):
#
#	def __getattr__(self, attr):
#		forbidden = ['originclass','_batch_exp', 'commit_to_db', 'commit_data_to_db', 'continue_exp_until', 'graph']
#		if attr not in forbidden:
#			return self.originclass.__getattr__(self, attr)
#		else:
#			return self.__getattribute__(self, attr)
#
#	def continue_exp_until(self):
#
#	def graph(self):
#
#	def commit_to_db(self, *args, **kwargs):
#		exp = copy.deepcopy(self)
#		self._batch_exp.uncontrol_exp(exp)
#		exp.commit_to_db(*args, **kwargs)
#
#	def commit_data_to_db(self, *args, **kwargs):
#		exp = copy.deepcopy(self)
#		self._batch_exp.uncontrol_exp(exp)
#		exp.commit_data_to_db(*args, **kwargs)
#
