from .job import Job
from ._example_job import ExampleJob
from .classic_job import ClassicJob, IteratedJob
from .experiment_job import ExperimentJob, GraphExpJob, ExperimentDBJob, GraphExpDBJob, MultipleGraphExpDBJob
from .cjob import CJob

import os
import jsonpickle

def run_job_from_path(path):
	file_path = os.path.join(path,'job.json')
	with open(file_path) as f:
		job = jsonpickle.loads(f.read())
	job.run()
