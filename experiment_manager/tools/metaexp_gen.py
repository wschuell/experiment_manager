# coding: utf-8

import os
import json

def get_file_content(filename,cache=None):
	if cache is None or filename not in list(cache.keys()):
		with open(filename,'r') as f:
			ans = f.read()
	else:
		ans = cache[filename]
	if cache is not None and filename not in list(cache.keys()):
		cache[filename] = ans
	return ans

def txt_to_dict(filename,cache=None):
	txt = get_file_content(filename=filename,cache=cache)
	lines = txt.split('\n')
	ans = {}
	txt_str = ''
	key = ''
	for l in lines:
		if len(l)>=3 and l[0] == '#' and l[-1] == '#':
			if key != '':
				ans[key] = txt_str
			txt_str = ''
			key = l.replace('#','')
		else:
			txt_str += l + '\n'
	if key != '':
		ans[key] = txt_str
	return ans


def dict_to_txt(filename,in_dict):
	with open(filename,'w') as f:
		f.write('\n'.join(['#####'+k+'#####\n\n'+v for k,v in list(in_dict.items())]))

def render(input_string,params_list):
	ans = input_string
	inner_params_list = [s.split(' %}}')[0].split(',') for s in input_string.split('{{% ')[1:]]
	for p,val in inner_params_list:
		if p in params_list:
			ans = ans.replace('{{% '+p+','+val+' %}}',p)
		else:
			ans = ans.replace('{{% '+p+','+val+' %}}',val)
	return ans


def auto_gen(folder,exec_type,plt_settings,func_type,tmax_type,nbiter,params,metrics_local,metrics_global,additional_metrics,imports,figures_dir=False,cache=None,foldername_venv=True,stop_on='default'):
	if not os.path.exists(folder):
		os.makedirs(folder)

	exec_str = txt_to_dict('configs/exec.py',cache=cache)[exec_type]
	plt_str = txt_to_dict('configs/plt_settings.py',cache=cache)[plt_settings]
	additional_metrics_str = txt_to_dict('configs/additional_metrics.py',cache=cache)[additional_metrics]
	tmax = txt_to_dict('configs/tmax.py',cache=cache)[tmax_type]
	cfg_func = txt_to_dict('configs/cfg_func.py',cache=cache)[func_type]
	stop_on_str = txt_to_dict('configs/stop_on.py',cache=cache)[stop_on]


	gen_fig = txt_to_dict('configs/gen_figs_list.py',cache=cache)

	metrics_all = json.loads(get_file_content('configs/metrics.json',cache=cache))

	metrics_list = [str(metrics_all[m]) for m in metrics_local]
	metrics_str = 'local_measures = {'+',\n                  '.join(['\''+str(m)+'\':'+str(metrics_all[m]) for m in metrics_local])+'\n                  }'

	metrics_list = [str(metrics_all[m]) for m in metrics_global]
	metrics_str += '\n\nglobal_measures = {'+',\n                   '.join(['\''+str(m)+'\':'+str(metrics_all[m]) for m in metrics_global])+'\n                   }'

	params_all = json.loads(get_file_content('configs/params.json',cache=cache))
	params_list = [params_all[p] for p in params]
	param_names = [params_all[p]['param_name'] for p in params]
	for pl in params_list:
		del pl['param_name']
	params_info = zip(param_names,params_list)
	params_str = 'params = {'+',\n          '.join(['\''+str(n)+'\':'+str(p) for n,p in params_info])+'\n          }'

	if foldername_venv:
		exec_str = exec_str.replace("'virtual_env':'","'virtual_env':'"+os.path.basename(folder))

	format_dict = {
			'imports':'\n'.join(imports),
			'nbiter':nbiter,
			'exec_str':exec_str,
			'func_str':'def xp_cfg('+','.join(param_names)+'):\n'+render(cfg_func,param_names),
			'Tmax_str':'def Tmax_func('+','.join(param_names)+'):\n'+render(tmax,param_names),
			'params':params_str,
			'metrics':metrics_str,
			'plt_func':plt_str,
			'additional_metrics':additional_metrics_str,
			'stop_on_str':stop_on_str,
		}

	main_str = """
import os

import math
import numpy as np

import experiment_manager as xp_man
{imports}

from experiment_manager.metaexp.metaexp import MetaExperiment


#### Function to determine the number of time steps for each simulation ####

{Tmax_str}

#### Function to construct the configuration of each simulation, depending on a few parameters, described below ####

{func_str}



#### Number of trials per distinct configuration ####

nbiter = {nbiter}


#### Description of the parameters of experiment configuration ####

{params}


#### Measures, to be found in naminggamesal.ngmeth ####

{metrics}

#### Additional measures ####

{additional_metrics}

#### Stopping condition for jobs ####

{stop_on_str}


#### Defining the MetaExperiment object, containing all this information ####

meta_exp = MetaExperiment(params=params,
              local_measures=local_measures,
              global_measures=global_measures,
              xp_cfg=xp_cfg,
              Tmax_func=Tmax_func,
              default_nbiter=nbiter,
              time_label='$t$',
              time_short_label='$t$',
              #time_max=80000,
              additional_metrics=additional_metrics,
              stop_on=stop_on,
              time_min=0)

#### Parameters for running the simulations. By default, using all available cores on local computer ####

db = ngal.ngdb.NamingGamesDB(do_not_close=True)
meta_exp.set_db(db)

{exec_str}


##### Making matplotlib more readable #####

{plt_func}


if __name__ == '__main__':
	with open('metaexp_running','a'):
		os.utime('metaexp_running',None)
	meta_exp.run()
	os.remove('metaexp_running')
	""".format(**format_dict)

	with open(folder+'/metaexp_settings.py','w') as f:
		f.write(main_str)

	if folder in list(gen_fig.keys()):
		with open(folder+'/gen_figs.py','w') as f:
			f.write(gen_fig['header']+gen_fig[folder])

	if figures_dir and not os.path.exists(folder+'/figures'):
		os.makedirs(folder+'/figures')


class ConfigGenerator(object):

	def __init__(self):
		self.gen_cache = {}

	def empty_cache(self):
		self.gen_cache = {}

	def auto_gen(self,**cfg):
		auto_gen(cache=self.gen_cache,**cfg)
