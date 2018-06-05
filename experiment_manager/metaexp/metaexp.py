import copy
import numpy as np
from scipy.optimize import curve_fit

from ..batchexp.batchexp import BatchExp

def dbcheck(func):
	def dbcheckobj(obj_self,*args,**kwargs):
		if obj_self.db is None:
			raise ValueError('No database set, please use set_db method to set it')
		else:
			return func(obj_self,*args,**kwargs)
	return dbcheckobj

def number_str(number):
	try:
		return str(round(number,int(-round(np.log10(number))+2)))
	except:
		return 'NaN'

def powerlaw_loglogfit(X,Y):
	def powerlaw(logx,A,k):
		return np.log(A) + k*logx
	index_list = [index for index in range(len(Y)) if np.isfinite(Y[index])]
	_Y = [Y[i] for i in index_list]
	_X = [X[i] for i in index_list]
	logX = np.log(_X)
	logY = np.log(_Y)
	init_vals = [1, 0]
	if len(logY) < 2:
		best_vals = np.array(init_vals)
	elif len(logY) == 2:
		_k = np.log(_Y[0]/float(_Y[1]))/np.log(_X[0]/float(_X[1]))
		_A = _Y[0]/float(_X[0]**_k)
		best_vals = (_A,_k)
	else:
		try:
			best_vals, covar = curve_fit(powerlaw, xdata=logX, ydata=logY, p0=init_vals)
		except ValueError:
			raise ValueError(str(logX)+'\n'+str(logY)+'\n'+str(X)+'\n'+str(Y)+'\n'+str(_X)+'\n'+str(_Y))
	logY_fit = powerlaw(logX,*best_vals)
	logYbar = np.sum(logY)/len(logY)
	ssreg = np.sum((logY_fit-logYbar)**2)
	sstot = np.sum((logY - logYbar)**2)
	r2 = ssreg / sstot
	return best_vals, r2

class MetaExperiment(object):
	def __init__(self,params,local_measures,global_measures,xp_cfg,Tmax_func,default_nbiter=1,time_label='Time',time_short_label='t'):
		self.params = copy.deepcopy(params)
		self.local_measures = copy.deepcopy(local_measures)
		self.global_measures = copy.deepcopy(global_measures)
		self._xp_cfg = copy.deepcopy(xp_cfg)
		self._Tmax_func = copy.deepcopy(Tmax_func)
		self.default_nbiter = default_nbiter
		self.db = None
		self.time_label = time_label
		self.time_short_label = time_short_label
		self.default_batch = 'nobatch'
		self.batches = {'nobatch':'nobatch'}

		for k,v in list(self.params.items()):
			test1 = 'default_value' not in list(self.params[k].keys())
			test2 = 'values' not in list(self.params[k].keys())
			test3 = 'label' not in list(self.params[k].keys())
			test4 = 'short_label' not in list(self.params[k].keys())
			if test1 and test2:
				raise ValueError('No values or default value provided for parameter '+str(k))
			elif test1:
				self.params[k]['default_value'] = self.params[k]['values'][0]
			elif test2:
				self.params[k]['values'] = [self.params[k]['default_value']]
			if test3:
				self.params[k]['label'] = str(k)
			if test4:
				self.params[k]['short_label'] = str(k)

	def Tmax(self,**subparams):
		_subparams = copy.deepcopy(subparams)
		for k,v in list(self.params.items()):
			if k not in list(_subparams.keys()):
				_subparams[k] = copy.deepcopy(v['default_value'])
			if isinstance(_subparams[k],list):
				raise TypeError('Parameter '+str(k)+' was provided several values instead of one: '+str(v))
		return self._Tmax_func(**_subparams)

	def xp_cfg(self,**subparams):
		_subparams = copy.deepcopy(subparams)
		for k,v in list(self.params.items()):
			if k not in list(_subparams.keys()):
				_subparams[k] = copy.deepcopy(v['default_value'])
			if isinstance(_subparams[k],list):
				raise TypeError('Parameter '+str(k)+' was provided several values instead of one: '+str(v))
		return self._xp_cfg(**_subparams)

	def set_db(self,db):
		self.db = db

	@dbcheck
	def plot(self,measure,nbiter=None,get_object=False,**subparams):
		if nbiter is None:
			nbiter = self.default_nbiter
		_subparams = copy.deepcopy(subparams)
		for k,v in list(self.params.items()):
			if k not in list(_subparams.keys()):
				_subparams[k] = copy.deepcopy(v['default_value'])
			if isinstance(_subparams[k],list):
				return self.plot_several(measure=measure,nbiter=nbiter,get_object=get_object,**subparams)
				#raise ValueError('Parameter '+str(k)+' has several values: '+str(_subparams[k])+'. Use plot_several to use different parameter values.')
		cfg = self.xp_cfg(**_subparams)
		xp_uuid = self.db.get_graph_id_list(method=measure,xp_cfg=cfg)[:nbiter]
		if len(xp_uuid)<nbiter:
			raise ValueError('Only '+str(len(xp_uuid))+' experiments available in the database, nbiter=' +str(nbiter)+ ' asked')
		gr = self.db.get_graph(method=measure,xp_uuid=xp_uuid[0])
		for i in range(nbiter-1):
			gr.add_graph(self.db.get_graph(method=measure,xp_uuid=xp_uuid[i+1]))
		gr.merge()
		try:
			gr.title = self.local_measures[measure]
		except:
			try:
				gr.title = self.global_measures[measure]
			except:
				gr.title = measure
		gr.xlabel = self.time_label
		if get_object:
			return gr
		else:
			gr.show()

	@dbcheck
	def plot_single(self,*args,**kwargs):
		return self.plot(*args,nbiter=1,**kwargs)

	@dbcheck
	def plot_against_single(self,*args,**kwargs):
		return self.plot_against(*args,nbiter=1,**kwargs)


	@dbcheck
	def plot_several(self,measure,nbiter=None,get_object=False,**subparams):
		if nbiter is None:
			nbiter = self.default_nbiter
		_subparams = copy.deepcopy(subparams)
		varying_params = []
		for k,v in list(self.params.items()):
			if k not in list(_subparams.keys()):
				_subparams[k] = copy.deepcopy(v['default_value'])
			if isinstance(_subparams[k],list):
				varying_params.append(k)
		configs = []
		if varying_params == []:
			return self.plot(measure=measure,nbiter=nbiter,get_object=get_object,**subparams)
		for k in varying_params:
			if configs == []:
				configs = [{k:v} for v in _subparams[k]]
			else:
				configs_bis = []
				for c in configs:
					for v in _subparams[k]:
						c2 = copy.deepcopy(c)
						c2[k] = v
						configs_bis.append(c2)
				configs = configs_bis
		_subparams_bis = copy.deepcopy(_subparams)
		_subparams_bis.update(configs[0])
		gr = self.plot(measure=measure,get_object=True,nbiter=nbiter,**_subparams_bis)
		if len(configs) > 1:
			for c in configs[1:]:
				_subparams_bis = copy.deepcopy(_subparams)
				_subparams_bis.update(c)
				gr2 = self.plot(measure=measure,get_object=True,nbiter=nbiter,**_subparams_bis)
				gr.add_graph(gr2)
		gr.legendoptions['labels'] = [', '.join([self.params[k]['short_label']+'='+str(c[k]) for k in varying_params]) for c in configs]
		if get_object:
			return gr
		else:
			gr.show()

	@dbcheck
	def plot_against(self,token,measure,nbiter=None,get_object=False,**subparams):
		if not token in self.params.keys():
			raise ValueError('Unknown parameter: '+str(token))
		if token not in list(subparams.keys()) or subparams[token] == 'all':
			token_values = self.params[token]['values']
		else:
			token_values = subparams[token]
		_subparams = copy.deepcopy(subparams)
		_subparams[token] = token_values[0]
		gr = self.plot(measure=measure,nbiter=nbiter,get_object=True,**_subparams)
		for i in range(len(gr._X)):
			gr._X[i][0] = _subparams[token]
		for v in token_values[1:]:
			_subparams = copy.deepcopy(subparams)
			_subparams[token] = v
			gr2 = self.plot(measure=measure,nbiter=nbiter,get_object=True,**_subparams)
			for i in range(len(gr2._X)):
				gr2._X[i][0] = _subparams[token]
			gr.complete_with(gr2)
		gr.xlabel = self.params[token]['label']
		gr.xmin = min(gr._X[0])
		gr.xmax = max(gr._X[0])
		if get_object:
			return gr
		else:
			gr.show()


	@dbcheck
	def plot_bestparam(self,xtoken,ytoken,measure,type_optim,nbiter=None,get_object=False,get_vect=False,**subparams):
		sp = copy.deepcopy(subparams)
		for k in list(sp.keys()):
			if k not in [] and isinstance(sp[k],list):
				return self.plot_bestparam_several(xtoken=xtoken,ytoken=ytoken,measure=measure,type_optim=type_optim,nbiter=nbiter,get_object=get_object,get_vect=get_vect,**subparams)
				#TODO: get_vect for plot_bestparam_several
		gr = self.plot_against(token=xtoken,measure=measure,nbiter=nbiter,get_object=True,**subparams)
		assert type_optim in ['min','max']
		if ytoken not in list(subparams.keys()) or subparams[ytoken] == 'all':
			yvec = self.params[ytoken]['values']
		else:
			yvec = subparams[ytoken]
		best_param_vect = []
		#TODO: handle case with different _X vectors
		for gx in gr._X:
			assert gx == gr._X[0]
		for x in range(len(gr._X[0])):
			if type_optim == 'min':
				val = min([gr._Y[i][x] for i in range(len(gr._Y))])
			elif type_optim == 'max':
				val = max([gr._Y[i][x] for i in range(len(gr._Y))])
			y_v = [i for i in range(len(gr._Y)) if gr._Y[i][x]==val]
			if y_v:
				y = y_v[0]
			else:
				y = 0
			best_param_vect.append(yvec[y])
		if get_vect:
			return best_param_vect
		gr2 = copy.deepcopy(gr)
		gr2.merge()
		gr2.ymin = min(yvec)
		gr2.ymax = max(yvec)
		if 'labels' in list(gr2.legendoptions.keys()):
			del gr2.legendoptions['labels']
		gr2.stdvec = [[0 for _ in best_param_vect]]
		gr2._Y[0] = best_param_vect
		gr2.title = 'Best '+self.params[ytoken]['label']+' '+type_optim+'imizing '+self.global_measures[measure]
		if get_object:
			return gr2
		else:
			gr2.show()

	@dbcheck
	def plot_bestparam_several(self,xtoken,ytoken,measure,type_optim,nbiter=None,get_object=False,get_vect=False,**subparams):
		if nbiter is None:
			nbiter = self.default_nbiter
		_subparams = copy.deepcopy(subparams)
		varying_params = []
		for k,v in list(self.params.items()):
			if k not in [xtoken,ytoken]:
				if k not in list(_subparams.keys()):
					_subparams[k] = copy.deepcopy(v['default_value'])
				if isinstance(_subparams[k],list):
					varying_params.append(k)
		configs = []
		if varying_params == []:
			return self.plot_bestparam(xtoken=xtoken,ytoken=ytoken,measure=measure,type_optim=type_optim,nbiter=nbiter,get_object=get_object,**subparams)
		for k in varying_params:
			if configs == []:
				configs = [{k:v} for v in _subparams[k]]
			else:
				configs_bis = []
				for c in configs:
					for v in _subparams[k]:
						c2 = copy.deepcopy(c)
						c2[k] = v
						configs_bis.append(c2)
				configs = configs_bis
		_subparams_bis = copy.deepcopy(_subparams)
		_subparams_bis.update(configs[0])
		gr = self.plot_bestparam(xtoken=xtoken,ytoken=ytoken,measure=measure,type_optim=type_optim,get_object=True,nbiter=nbiter,**_subparams_bis)
		if len(configs) > 1:
			for c in configs[1:]:
				_subparams_bis = copy.deepcopy(_subparams)
				_subparams_bis.update(c)
				gr2 = self.plot_bestparam(xtoken=xtoken,ytoken=ytoken,type_optim=type_optim,measure=measure,get_object=True,nbiter=nbiter,**_subparams_bis)
				gr.add_graph(gr2)
		gr.legendoptions['labels'] = [', '.join([self.params[k]['short_label']+'='+str(c[k]) for k in varying_params]) for c in configs]
		if get_object:
			return gr
		else:
			gr.show()

	@dbcheck
	def run(self,batch=None,t=10,coeff=2,nbiter=None,**subparams):
		if nbiter is None:
			nbiter = self.default_nbiter
		_subparams = copy.deepcopy(subparams)
		for k in list(self.params.keys()):
			if k not in list(_subparams.keys()):
				_subparams[k] = copy.deepcopy(self.params[k]['values'])
			if not isinstance(_subparams[k],list):
				_subparams[k] = [_subparams[k]]
		if batch is None:
			batch = self.batches[self.default_batch]
		if batch == 'nobatch':
			configs = []
			for k in list(_subparams.keys()):
				if configs == []:
					configs = [{k:v} for v in _subparams[k]]
				else:
					configs_bis = []
					for c in configs:
						for v in _subparams[k]:
							c2 = copy.deepcopy(c)
							c2[k] = v
							configs_bis.append(c2)
					configs = configs_bis
			cfg_list = [self.xp_cfg(**c) for c in configs]
			for cfg,c in zip(cfg_list,configs):
				id_list = self.db.get_id_list(**cfg)
				if len(id_list)<nbiter:
					for i in range(nbiter-len(id_list)):
						xp = self.db.get_experiment(force_new=True,**cfg)
						id_list.append(xp.uuid)
				for xp_uuid in id_list:
					test = self.db.get_param(xp_uuid=xp_uuid, param='Tmax')>=self.Tmax(**c)
					for m in list(self.local_measures.keys())+list(self.global_measures.keys()):
						test = test and self.db.data_exists(xp_uuid=xp_uuid, method=m)
						test = test and self.db.get_param(xp_uuid=xp_uuid, param='Time_max',method=m)>=self.Tmax(**c)
					if not test:
						xp = self.db.get_experiment(xp_uuid=xp_uuid)
						xp.continue_exp_until(T=self.Tmax(**c))
						for m in  list(self.local_measures.keys())+list(self.global_measures.keys()):
							xp.graph(method=m)
		else:
			job_configs = []
			for k in list(_subparams.keys()):
				if job_configs == []:
					job_configs = [{k:v} for v in _subparams[k]]
				else:
					configs_bis = []
					for c in job_configs:
						for v in _subparams[k]:
							c2 = copy.deepcopy(c)
							c2[k] = v
							configs_bis.append(c2)
					job_configs = configs_bis
			job_cfg_list = [{'xp_cfg':self.xp_cfg(**c),'method': list(self.local_measures.keys())+list(self.global_measures.keys()),'tmax':self.Tmax(**_subparams),'nbiter':nbiter} for c in job_configs]
			batch.add_jobs(job_cfg_list)
			batch.job_queue.auto_finish_queue(t=t,coeff=coeff)
			#TODO: clear output

	def run_single(self,batch=None):
		self.run(batch=batch,nbiter=1)

	@dbcheck
	def set_batch(self,name=None,set_as_default=False,**batch_cfg):
		_batch_cfg = copy.deepcopy(batch_cfg)
		if name is None:
			try:
				name = _batch_cfg['name']
			except:
				name = _batch_cfg['jq_cfg']['jq_type']
		elif not _batch_cfg:
			_batch_cfg['jq_cfg']={'jq_type':name}
		self.batches[name] = BatchExp(**_batch_cfg)
		if set_as_default:
			self.default_batch = name

	def powerlaw_fit(self,graph,get_object=False,get_values=False):
		for i in range(len(graph._X)):
			x = graph._X[i]
			y = graph._Y[i]
			params,r2 = powerlaw_loglogfit(x,y)
			y_fit = params[0]*np.power(x,params[1])
			graph._X.append(x)
			graph._Y.append(y_fit)
			graph.stdvec.append([0]*len(x))
			options = copy.deepcopy(graph.Yoptions[i])
			options['linestyle'] = '--'
			options['color'] = 'black'
			graph.Yoptions.append(options)
			graph.loglog = True
			graph.legendoptions['labels'].append('y='+number_str(params[0])+'*x^'+number_str(params[1])+', R^2='+number_str(r2))
			#TODO: do not append but put right after curve, and put values in labels?
		if not get_values and not get_object:
			graph.show()
		elif get_object:
			return graph
		elif get_values:
			return params,r2
		else:
			raise ValueError('get_object and get_values cannot be set to True at the same time.')

