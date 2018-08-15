import pytest
import copy
import random
import numpy as np
import os
import time
import subprocess

import tempfile
import uuid

from experiment_manager.job_queue import get_jobqueue
from experiment_manager.job import get_job

newpath = tempfile.mkdtemp()
os.chdir(newpath)

import naminggamesal as ngal
db = ngal.ngdb.NamingGamesDB(do_not_close=True)

jq_cfg_list = [
	{'jq_type':'local'},
	{'jq_type':'local_multiprocess'},
	]

try:
	command = "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' docker_slurm"
	IP = subprocess.run(command.split(' '),shell=True,check=True,capture_output=True)
	jq_cfg_docker = {'jq_type':'slurm',
    'modules':[],
    #'virtual_env':virtualenv,
    #'requirements': [pip_arg_xp_man],
     'ssh_cfg':{
     'username':'root',
    'hostname':IP,
    'password':'dockerslurm',}
                }
	jq_cfg_list.append(jq_cfg_docker)
except:
	pass

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

@pytest.fixture(params=list(range(4)))
def job(request):
	ind = request.param
	if ind == 0:
		return get_job(**{'job_type':'example_job'})
	elif ind == 1:
		db.reconnect()
		xp = db.get_experiment(force_new=True)
		return get_job(**{'job_type':'experiment_job','xp_uuid':xp.uuid,'tmax':10})
	elif ind == 2:
		db.reconnect()
		xp = db.get_experiment(force_new=True)
		return get_job(**{'job_type':'experiment_job_multigraph','xp_uuid':xp.uuid,'tmax':10,'method':['srtheo','N_d','N_words','N_meanings','conv_time2']})
	elif ind == 3:
		db.reconnect()
		xp = db.get_experiment(force_new=True)
		return get_job(**{'job_type':'experiment_job_nostorage','xp_uuid':xp.uuid,'tmax':10,'method':['srtheo','N_d','N_words','N_meanings','conv_time2']})
	else:
		raise ValueError('No jobs for value:',ind)

def test_jobs(job,jq):
	jq.add_job(job)
	jq.auto_finish_queue(t=3)

def test_job_checkpoint(job,jq):
	job.estimated_time = 2
	jq.add_job(job)
	jq.update_queue()
	time.sleep(1.2)
	jq.kill()
	jq.auto_finish_queue(t=3)
