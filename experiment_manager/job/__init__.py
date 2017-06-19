from .job import Job
from ._example_job import ExampleJob
from .classic_job import ClassicJob, IteratedJob
from .experiment_job import ExperimentJob, GraphExpJob, ExperimentDBJob, GraphExpDBJob, MultipleGraphExpDBJob
from .cjob import CJob

import os
import time
import jsonpickle

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
