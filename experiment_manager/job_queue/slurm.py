
from .cluster import ClusterJobQueue

class SlurmJobQueue(ClusterJobQueue):

	def gen_files(self, format_dict):
		if 'multijob_dir' in format_dict.keys():
			return [('script.py',self.multijob_script(format_dict=format_dict)),
					('epilogue.sh',self.multijob_epilogue(format_dict=format_dict))]
		else:
			return [('script.py',self.individual_script(format_dict=format_dict)),
					('epilogue.sh',self.individual_epilogue(format_dict=format_dict))]




	def individual_launch_script(self, format_dict):
		return """#!/usr/bin/bash
#SBATCH --time={walltime}
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name="{job_name}"
#SBATCH --output={job_dir}/output.txt
#SBATCH --error={job_dir}/error.txt

./WHERE/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2

echo "Job finished, backing up files."
PBS_JOBID=$1
cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir
cp -f -R {base_work_dir}\"$PBS_JOBID\"/* {job_dir}/
rm -R {base_work_dir}$PBS_JOBID
echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict)

	def individual_script(self, format_dict):
		return """#!{python_bin}

import os
import sys
import shutil
import jsonpickle

SLURM_JOBID = os.environ['SLURM_JOB_ID']
job_dir = '{job_dir}'
work_dir = '{base_work_dir}'+SLURM_JOBID

shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict)

	def individual_epilogue(self, format_dict):
		return """#!/bin/bash
echo "Job finished, backing up files."
PBS_JOBID=$1
cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir
cp -f -R {base_work_dir}\"$PBS_JOBID\"/* {job_dir}/
rm -R {base_work_dir}$PBS_JOBID
echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict)

	def multijob_script(self, format_dict):
		return """#!{python_bin}

import os
import sys
import shutil
import jsonpickle

import threading

SLURM_JOBID = os.environ['SLURM_ARRAY_JOB_ID']+'_'+os.environ['SLURM_ARRAY_TASK_ID']
SLURM_ARRAYID = os.environ['SLURM_ARRAY_TASK_ID']

jobdir_dict = {jobdir_dict}

job_dir = jobdir_dict[int(SLURM_ARRAYID)]
work_dir = '{base_work_dir}'+SLURM_JOBID


shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'

jobthread = threading.Thread(name='job', target=job.run)
jobthread.setDaemon(True)
jobthread.start()
jobthread.join()


sys.exit(0)
""".format(**format_dict)




	def multijob_launch_script(self, format_dict):
		return """#!/usr/bin/bash
#SBATCH --time={walltime}
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name="{multijob_name}"
#SBATCH --output={multijob_dir}/output.txt
#SBATCH --error={multijob_dir}/error.txt

./WHERE/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2

echo "Job finished, backing up files."
PBS_JOBID=$1

MULTIJOBDIR={multijob_dir}
ARRAYID=$(python -c "jobid='"$PBS_JOBID"'; print jobid.split('[')[1].split(']')[0]")
JOBDIR=$(python -c "jobdir_dict = {jobdir_dict}; print jobdir_dict["$ARRAYID"]")

cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir

cp -f -R {base_work_dir}\"$PBS_JOBID\"/* $JOBDIR/
rm -R {base_work_dir}$PBS_JOBID



echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict)

	def multijob_epilogue(self, format_dict):
		return """#!/bin/bash
echo "Job finished, backing up files."
PBS_JOBID=$1

MULTIJOBDIR={multijob_dir}
ARRAYID=$(python -c "jobid='"$PBS_JOBID"'; print jobid.split('[')[1].split(']')[0]")
JOBDIR=$(python -c "jobdir_dict = {jobdir_dict}; print jobdir_dict["$ARRAYID"]")

cp -R {base_work_dir}\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}\"$PBS_JOBID\"/backup_dir

cp -f -R {base_work_dir}\"$PBS_JOBID\"/* $JOBDIR/
rm -R {base_work_dir}$PBS_JOBID



echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $1"
echo "User ID: $2"
echo "Group ID: $3"
echo "Job Name: $4"
echo "Session ID: $5"
echo "Resource List: $6"
echo "Resources Used: $7"
echo "Queue Name: $8"
echo "Account String: $9"
echo "================================"
exit 0
""".format(**format_dict)

	def send_submit_command(self,cmd_type,format_dict=None,t_min=None,output_path=None,file_path=None):
		session = self.ssh_session
		if cmd_type == 'simple':
			return session.command_output('sbatch --time='+str(t_min)+' -o '+output_path+' '+file_path)[:-1]
		elif cmd_type == 'single_job':
			return session.command_output('sbatch -l epilogue={job_dir}/epilogue.sh {job_dir}/script.py'.format(**format_dict))[:-1]
		elif cmd_type == 'multijob':
			return session.command_output('sbatch --array=1-{Njobs} -l epilogue={multijob_dir}/epilogue.sh {multijob_dir}/script.py'.format(**format_dict))[:-1]

	def array_jobid(self,jobid,jobN):
		return jobid + '_' + str(jobN)

	def get_running_jobs_string(self):
		session = self.ssh_session
		return session.command_output('squeue -r -o %18i -u '+self.ssh_cfg['username'])

	def count_running_jobs(self):
		session = self.ssh_session
		count = int(session.command_output('squeue -u {} -r|wc -l'.format(self.ssh_cfg['username'])))
		if count > 0:
			count -= 1
		return count

	def cancel_command(self,jobid):
		return 'scancel ' + str(jobid)

	def output_killed_string(self):
		return 'DUE TO TIME LIMIT:'
