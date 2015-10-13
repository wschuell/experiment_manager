from .. import ngdb.NamingGamesDB as Database
from . import job_queue.JobQueue as JobQueue
import cPickle
import uuid
import copy

class BatchExp(object):

	def __init__(self, filename=None, db=None, uuid=None, def_meth=None, secondary_dbs=[], jq_cfg={'jq_type':'local'}):
		if filename:
			with open(filename+'.b', 'wb') as f:
				unpickled = cPickle.loads(bz2.decompress(f.read()))
			self.__dict__ = copy.deepcopy(unpickled.__dict__)
		else:
			self.uuid = str(uuid.uuid1())
			if db:
				self.db = db
			else:
				self.db = Database(path=self.uuid+'.db')
			if type(secondary_dbs) is not list:
				self.secondary_dbs = [secondary_dbs]
			else:
				self.secondary_dbs = secondary_dbs
			self.job_queue = JobQueue(jq_cfg**)

	def get_uuid(self, xp_cfg, blacklist=[], T=0):
		SEARCH MAINDB
		if FOUND:
			return exp.uuid
		SEARCH SEC DB
		if FOUND:
			COPYTOMAIN
			return exp.uuid
		return GETEXP_NEW.uuid

	def get_exp(self, xp_cfg, blacklist=[], T=0):
		GETEXP(self.get_uuid(xp_cfg=xp_cfg, blacklist=blacklist, T=T))

	def add_job(self, job_cfg):
		self.job_queue.add_job(Job(**job_cfg))

	def add_sec_db(self, db):
		if type(db) is list:
			self.secondary_dbs = self.secondary_dbs + db
		else:
			self.secondary_dbs.append(db)

	def remove_sec_db(self, db):
		if type(db) is list:
			for d in db:
				self.remove_sec_db(d)
		else:
			try:
				self.secondary_dbs.remove(db)
			except ValueError:
				pass

	def save(self, filename=None):
		if filename is None:
			filename = str(self.uuid)
		filename+='.b'
		with open(filename, 'wb') as f:
			f.write(bz2.compress(cPickle.dumps(self,cPickle.HIGHEST_PROTOCOL)))

	def include_db(self, db, erase=False):
		MERGE DB WITH MAIN
		if erase:
			REMOVE DB

	def run(self,t=60):
		self.job_queue.auto_finish_queue(t=t)




	def graph_exp(self, uuid, blacklist=[]):
		GRAPHEXP
		return graph

	def batch_run(self, xp_cfg_list, blacklist=[], T=0):
		xp_list=[]
		for cfg in xp_cfg_list:
			xp_list.append(self.get_uuid(xp_cfg=cfg, blacklist=blacklist, T=T))
		for uuid in xp_list:
			self.run_exp(uuid=uuid, T=T)
		CHECKEXP

	def get_batch_graph(self, BATCHGRAPHMETHOD, blacklist=[]):
		BATCHGRAPH
		return graph

