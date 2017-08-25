
from .cluster import ClusterJobQueue

class SlurmJobQueue(ClusterJobQueue):

	def gen_files(self, format_dict):
		if 'multijob_dir' in format_dict.keys():
			return [('script.py',self.multijob_script(format_dict=format_dict)),
					('launch_script.sh',self.multijob_launch_script(format_dict=format_dict)),
					('multijob.json',self.multijob_json(format_dict=format_dict))]
		else:
			return [('script.py',self.individual_script(format_dict=format_dict)),
					('launch_script.sh',self.individual_launch_script(format_dict=format_dict))]




	def individual_launch_script(self, format_dict):
		return """#!/bin/bash
#SBATCH --time={walltime}
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name="{job_name}"
#SBATCH --output={job_dir}/output.txt
#SBATCH --error={job_dir}/error.txt

echo "Starting Job"

chmod u+x {job_dir}/script.py && {job_dir}/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Reaching time limit: Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2;

echo "Job finished, backing up files."

JOBID=$SLURM_JOBID

if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}\"$JOBID\"/
fi
rm -R {base_work_dir}/\"$JOBID\"/backup_dir
fi

rm {base_work_dir}/\"$JOBID\"/error.txt
rm {base_work_dir}/\"$JOBID\"/output.txt

cp -f -R {base_work_dir}/\"$JOBID\"/* {job_dir}/
rm -R {base_work_dir}/$JOBID

echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $SLURM_JOB_ID"
echo "User ID: $SLURM_JOB_ACCOUNT"
echo "Node List: $SLURM_JOB_NODE_LIST"
echo "Job Name: $SLURM_JOB_NAME"
echo "Submit Dir: $SLURM_SUBMIT_DIR"
echo "Submit Host: $SLURM_SUBMIT_HOST"
echo "Node Name: $SLURMD_NODENAME"
echo "================================"
scontrol show job $SLURM_JOB_ID

exit 0
""".format(**format_dict)

	def individual_script(self, format_dict):
		return """#!{python_bin}

import os
import sys
import shutil
import jsonpickle


SLURM_JOBID = os.environ['SLURM_JOBID']
job_dir = '{job_dir}'
work_dir = os.path.join('{base_work_dir}',SLURM_JOBID)

shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()


sys.exit(0)
""".format(**format_dict)

	def multijob_script(self, format_dict):
		return """#!{python_bin}

import os
import sys
import shutil
import jsonpickle


SLURM_JOBID = os.environ['SLURM_ARRAY_JOB_ID']+'_'+os.environ['SLURM_ARRAY_TASK_ID']
SLURM_ARRAYID = os.environ['SLURM_ARRAY_TASK_ID']

jobdir_dict = {jobdir_dict}

job_dir = jobdir_dict[int(SLURM_ARRAYID)]
work_dir = os.path.join('{base_work_dir}',SLURM_JOBID)


shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict)




	def multijob_launch_script(self, format_dict):
		return """#!/bin/bash -i
#SBATCH --time={walltime}
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name="{multijob_name}"
#SBATCH --output={multijob_dir}/output.txt
#SBATCH --error={multijob_dir}/error.txt

ARRAYID=$SLURM_ARRAY_TASK_ID
JOBID=\"$SLURM_ARRAY_JOB_ID\"_\"$ARRAYID\"

echo "Starting Job"

chmod u+x {multijob_dir}/script.py && {multijob_dir}/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Reaching time limit: Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2

echo "Job finished, backing up files."


MULTIJOBDIR={multijob_dir}
JOBDIR=$(python -c "import json; f = open('{multijob_dir}/multijob.json','r');jobdir_dict = json.loads(f.read()); f.close(); print jobdir_dict["$ARRAYID"]")



if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}\"$JOBID\"/
fi
rm -R {base_work_dir}/\"$JOBID\"/backup_dir
fi


cp -f -R {base_work_dir}/\"$JOBID\"/* $JOBDIR/
rm -R {base_work_dir}/$JOBID


echo "Backup done"
echo "================================"
echo "SLURM EPILOGUE"
echo "================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Job Array ID: $SLURM_ARRAY_JOB_ID"
echo "Job Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "User ID: $SLURM_JOB_ACCOUNT"
echo "Node List: $SLURM_JOB_NODE_LIST"
echo "Job Name: $SLURM_JOB_NAME"
echo "Submit Dir: $SLURM_SUBMIT_DIR"
echo "Submit Host: $SLURM_SUBMIT_HOST"
echo "Node Name: $SLURMD_NODENAME"
echo "================================"
scontrol show job $SLURM_JOB_ID

exit 0
""".format(**format_dict)

#cp -R {base_work_dir}\"$JOBID\"/backup_dir/{job_uuid} {job_backup_dir}
#rm -R {base_work_dir}\"$JOBID\"/backup_dir



	def send_submit_command(self,cmd_type,format_dict=None,t_min=None,output_path=None,file_path=None):
		session = self.ssh_session
		if cmd_type == 'simple':
			return session.command_output('sbatch --time='+str(t_min)+' -o '+output_path+' '+file_path,bashrc=True)[:-1]
		elif cmd_type == 'single_job':
			return session.command_output('sbatch {job_dir}/launch_script.sh'.format(**format_dict),bashrc=True)[:-1]
		elif cmd_type == 'multijob':
			return session.command_output('sbatch --array=1-{Njobs} {multijob_dir}/launch_script.sh'.format(**format_dict),bashrc=True)[:-1]

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

	def jobid_from_submit_output(self,submit_output):
		return submit_output.split(' ')[-1]


	def multijob_json(self, format_dict):
		return """{jobdir_dict}
		""".format(**format_dict)



class OldSlurmJobQueue(SlurmJobQueue):


	def get_running_jobs_string(self):
		session = self.ssh_session
		return session.command_output('squeue -o %18i -u '+self.ssh_cfg['username'])


	def submit_job(self, job):
		self.individual_submit_job(job)

	def global_submit(self):
		pass
