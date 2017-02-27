
from .cluster import ClusterJobQueue

class TorqueJobQueue(ClusterJobQueue):

	def gen_files(self, format_dict):
		if 'multijob_dir' in format_dict.keys():
			return [('script.py',self.multijob_script(format_dict=format_dict)),
					('epilogue.sh',self.multijob_epilogue(format_dict=format_dict))]
		else:
			return [('script.py',self.individual_script(format_dict=format_dict)),
					('epilogue.sh',self.individual_epilogue(format_dict=format_dict))]

	def individual_script(self, format_dict):
		return """#!{python_bin}
#PBS -o {job_dir}/output.txt
#PBS -e {job_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {job_name}


import os
import sys
import shutil
import jsonpickle

PBS_JOBID = os.environ['PBS_JOBID']
job_dir = '{job_dir}'
work_dir = os.path.join('{base_work_dir}',PBS_JOBID)

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
cp -R {base_work_dir}/\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}/\"$PBS_JOBID\"/backup_dir
cp -f -R {base_work_dir}/\"$PBS_JOBID\"/* {job_dir}/
rm -R {base_work_dir}/$PBS_JOBID
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
#PBS -o {multijob_dir}/output.txt
#PBS -e {multijob_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {multijob_name}


import os
import sys
import shutil
import jsonpickle

PBS_JOBID = os.environ['PBS_JOBID']
PBS_ARRAYID = os.environ['PBS_ARRAYID']

jobdir_dict = {jobdir_dict}

job_dir = jobdir_dict[int(PBS_ARRAYID)]
work_dir = os.path.join('{base_work_dir}',PBS_JOBID)


shutil.copytree(job_dir, work_dir)
os.chdir(work_dir)

with open('job.json','r') as f:
	job = jsonpickle.loads(f.read())

job.path = '.'
job.run()

sys.exit(0)
""".format(**format_dict)

	def multijob_epilogue(self, format_dict):
		return """#!/bin/bash
echo "Job finished, backing up files."
PBS_JOBID=$1

MULTIJOBDIR={multijob_dir}
ARRAYID=$(python -c "jobid='"$PBS_JOBID"'; print jobid.split('[')[1].split(']')[0]")
JOBDIR=$(python -c "jobdir_dict = {jobdir_dict}; print jobdir_dict["$ARRAYID"]")

cp -R {base_work_dir}/\"$PBS_JOBID\"/backup_dir/* {basedir}/backup_dir/
rm -R {base_work_dir}/\"$PBS_JOBID\"/backup_dir

cp -f -R {base_work_dir}/\"$PBS_JOBID\"/* $JOBDIR/
rm -R {base_work_dir}/$PBS_JOBID



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
			return session.command_output('qsub -l walltime=00:'+str(t_min)+':00 -l nodes=1:ppn=1 -j oe -o '+output_path+' '+file_path)[:-1]
		elif cmd_type == 'single_job':
			return session.command_output('qsub -l epilogue={job_dir}/epilogue.sh {job_dir}/script.py'.format(**format_dict))[:-1]
		elif cmd_type == 'multijob':
			return session.command_output('qsub -t 1-{Njobs} -l epilogue={multijob_dir}/epilogue.sh {multijob_dir}/script.py'.format(**format_dict))[:-1]

	def array_jobid(self,jobid,jobN):
		return jobid.split('[')[0] + '[' + str(jobN) + ']' + jobid.split(']')[1]

	def get_running_jobs_string(self):
		session = self.ssh_session
		return session.command_output('qstat -f -t|grep \'Job Id:\'')

	def count_running_jobs(self):
		session = self.ssh_session
		qstat = int(session.command_output('qstat -u {} -t|wc -l'.format(self.ssh_cfg['username'])))
		if qstat > 0:
			qstat -= 5
		return qstat

	def cancel_command(self,jobid):
		return 'qdel ' + str(jobid)

	def output_killed_string(self):
		return 'PBS: job killed:'
