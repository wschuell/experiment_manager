import pytest
import copy
import random
import numpy as np
import os

import tempfile
import uuid

from experiment_manager.job_queue import get_jobqueue
from experiment_manager.job import get_job

newpath = tempfile.mkdtemp()
os.chdir(newpath)

import naminggamesal as ngal

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

#job_cfg_list = [
#	{'job_type':'example_job'},
#	{'job_type':'experiment_job','xp_cfg':{},'tmax':10},
#	]

@pytest.fixture(params=list(range(2)))
def job(request):
	ind = request.param
	if ind == 0:
		return get_job(**{'job_type':'example_job'})
	elif ind == 1:
		db = ngal.ngdb.NamingGamesDB()
		xp = db.get_experiment(force_new=True)
		return get_job(**{'job_type':'experiment_job','xp_uuid':xp.uuid,'tmax':10})
	else:
		raise ValueError('No jobs for value:',ind)

def test_jobs(job,jq):
	jq.add_job(job)
	jq.auto_finish_queue(t=1)
