from .job import Job
from ._example_job import ExampleJob
#from .classic_job import ClassicJob, IteratedJob
#from .experiment_job import ExperimentJob, GraphExpJob, ExperimentDBJob, GraphExpDBJob, MultipleGraphExpDBJob
#from .cjob import CJob

from importlib import import_module

import os
import time
import jsonpickle


job_class={
	'example_job':'_example_job.ExampleJob',
	'cjob':'cjob.CJob',
	'classic_job':'classic_job.ClassicJob',
	'iterated_job':'classic_job.IteratedJob',
	'notebook_job':'notebook_job.NotebookJob',
	'experiment_job':'experiment_job.ExperimentDBJob',
	'experiment_job_nostorage':'experiment_job.ExperimentDBJobNoStorage',
	'experiment_job_multigraph':'experiment_job.MultipleGraphExpDBJob',
	}

def get_job(job_type='example_job', **job_cfg2):
	tempstr = job_type
	if tempstr in list(job_class.keys()):
		tempstr = job_class[tempstr]
	templist = tempstr.split('.')
	temppath = '.'.join(templist[:-1])
	tempclass = templist[-1]
	_tempmod = import_module('.'+temppath,package=__name__)
	job = getattr(_tempmod,tempclass)(**job_cfg2)
	return job

def run_job_from_path(path):
	file_path = os.path.join(path,'job.json')
	if not os.path.exists(file_path):
		raise OSError('file doesnt exist: '+filepath)
	with open(file_path,'r') as f:
		job_json = f.read()
	if not job_json:
		time.sleep(0.5) # if concurrent access to the file job.json
		with open(file_path,'r') as f:
			job_json = f.read()
	job = jsonpickle.loads(job_json)
	job.run()
