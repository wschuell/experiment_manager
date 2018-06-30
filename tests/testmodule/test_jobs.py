import pytest
import copy
import random
import numpy as np
import os

import tempfile

from experiment_manager.job_queue import get_jobqueue
from experiment_manager.job import get_job
newpath = tempfile.mkdtemp()
os.chdir(newpath)


jq_cfg_list = [
	{'jq_type':'local'},
	{'jq_type':'local_multiprocess'},
	]

jq_list = [get_jobqueue(**cfg) for cfg in jq_cfg_list]

@pytest.fixture(params=jq_list)
def jq(request):
	jq = request.param
	jq.clean_jobqueue()
	return jq

job_cfg_list = [
	{'job_type':'example_job'},
	{'job_type':'experiment_job','xp_cfg':{},'tmax':10},
	]

@pytest.fixture(params=job_cfg_list)
def job_cfg(request):
	return request.param

def test_jobs(job_cfg,jq):
	j = get_job(**job_cfg)
	jq.add_job(j)
	jq.auto_finish_queue(t=1)
