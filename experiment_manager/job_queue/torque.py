
from .cluster import ClusterJobQueue

class TorqueJobQueue(ClusterJobQueue):

	def gen_files(self, format_dict):
		if not self.without_epilogue:
			if 'multijob_dir' in format_dict.keys():
				return [('script.py',self.multijob_script(format_dict=format_dict)),
						('epilogue.sh',self.multijob_epilogue(format_dict=format_dict)),
						('multijob.json',self.multijob_json(format_dict=format_dict))]
			else:
				return [('script.py',self.individual_script(format_dict=format_dict)),
						('epilogue.sh',self.individual_epilogue(format_dict=format_dict))]
		else:
			if 'multijob_dir' in format_dict.keys():
				return [('script.py',self.multijob_script(format_dict=format_dict)),
						('launch_script.sh',self.multijob_launch_script(format_dict=format_dict)),
						('multijob.json',self.multijob_json(format_dict=format_dict))]
			else:
				return [('script.py',self.individual_script(format_dict=format_dict)),
						('launch_script.sh',self.individual_launch_script(format_dict=format_dict))]

	def individual_script(self, format_dict):
		return """#!{python_bin}
#PBS -o {job_dir}/output.txt
#PBS -e {job_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {job_name}

print "Preparing Job"

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

print "Starting Job"

job.run()

print "Job finished"

sys.exit(0)
""".format(**format_dict)

	def individual_epilogue(self, format_dict):
		return """#!/bin/bash
echo "Job finished, backing up files.";
JOBID=$1

if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
echo "Copying backup_dir";
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}/\"$JOBID\"/;
fi
echo "Removing backup_dir";
rm -R {base_work_dir}/\"$JOBID\"/backup_dir;
fi

cp -f -R {base_work_dir}/\"$JOBID\"/* {job_dir}/;
rm -R {base_work_dir}/$JOBID;

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

print "Preparing Job"

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

print "Starting Job"

job.run()

print "Job finished"

sys.exit(0)
""".format(**format_dict)

	def multijob_epilogue(self, format_dict):
		return """#!/bin/bash
echo "Job finished, backing up files.";
JOBID=$1

MULTIJOBDIR={multijob_dir}
ARRAYID=$(python -c "jobid='"$JOBID"'; print jobid.split('[')[1].split(']')[0]")
JOBDIR=$(python -c "import json; f = open('multijob.json','r');jobdir_dict = json.loads(f.read()); f.close(); print jobdir_dict["$ARRAYID"]")


if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
echo "Copying backup_dir";
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}/\"$JOBID\"/;
fi
echo "Removing backup_dir";
rm -R {base_work_dir}/\"$JOBID\"/backup_dir;
fi

cp -f -R {base_work_dir}/\"$JOBID\"/* $JOBDIR/
rm -R {base_work_dir}/$JOBID



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
			return session.command_output('qsub -p +1000 -l walltime=00:'+str(t_min)+':00 -l nodes=1:ppn=1 -p +1023 -j oe -o '+output_path+' '+file_path)[:-1]
		elif cmd_type == 'single_job':
			if not self.without_epilogue:
				return session.command_output('qsub -l epilogue={job_dir}/epilogue.sh {job_dir}/script.py'.format(**format_dict))[:-1]
			else:
				return session.command_output('qsub {job_dir}/launch_script.sh'.format(**format_dict))[:-1]
		elif cmd_type == 'multijob':
			if not self.without_epilogue:
				return session.command_output('qsub -t 1-{Njobs} -l epilogue={multijob_dir}/epilogue.sh {multijob_dir}/script.py'.format(**format_dict))[:-1]
			else:
				return session.command_output('qsub -t 1-{Njobs} {multijob_dir}/launch_script.sh'.format(**format_dict))[:-1]

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


	def individual_launch_script(self, format_dict):
		return """#!/bin/bash
#PBS -o {job_dir}/output.txt
#PBS -e {job_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {job_name}

echo "Preparing Job"

JOBID=$PBS_JOBID


chmod u+x {job_dir}/script.py && {job_dir}/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Reaching time limit: Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2;

echo "Job finished, backing up files."

if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}\"$JOBID\"/
fi
rm -R {base_work_dir}/\"$JOBID\"/backup_dir
fi

#cp -f -R {base_work_dir}/\"$JOBID\"/* {job_dir}/
#rm -R {base_work_dir}/$JOBID

echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $PBS_JOBID"
echo "================================"

exit 0
""".format(**format_dict)


	def multijob_launch_script(self, format_dict):
		return """#!/bin/bash
#PBS -o {multijob_dir}/output.txt
#PBS -e {multijob_dir}/error.txt
#PBS -l walltime={walltime}
#PBS -l nodes=1:ppn=1
#PBS -N {multijob_name}

JOBID=$PBS_JOBID
ARRAYID=$PBS_ARRAYID

chmod u+x {multijob_dir}/script.py && {multijob_dir}/script.py &
PID=$!

WAIT_TIME=$(({walltime_seconds}-120))
sleep $WAIT_TIME && echo "Reaching time limit: Killing Job" && kill -9 $PID &
PID2=$!

wait $PID

kill -9 $PID2

echo "Job finished, backing up files.";

MULTIJOBDIR={multijob_dir}
JOBDIR=$(python -c "import json; f = open('multijob.json','r');jobdir_dict = json.loads(f.read()); f.close(); print jobdir_dict["$ARRAYID"]")


if [ -d {base_work_dir}/\"$JOBID\"/backup_dir ]; then
if [ ! -f {base_work_dir}/\"$JOBID\"/backup_dir/backup_lock/* ]; then
echo "Copying backup_dir";
cp -f -R {base_work_dir}/\"$JOBID\"/backup_dir/*/* {base_work_dir}/\"$JOBID\"/;
fi
echo "Removing backup_dir";
rm -R {base_work_dir}/\"$JOBID\"/backup_dir;
fi

cp -f -R {base_work_dir}/\"$JOBID\"/* $JOBDIR/
rm -R {base_work_dir}/$JOBID



echo "Backup done"
echo "================================"
echo "EPILOGUE"
echo "================================"
echo "Job ID: $JOBID"
echo "================================"

exit 0
""".format(**format_dict)

#cp -R {base_work_dir}\"$JOBID\"/backup_dir/{job_uuid} {job_backup_dir}
#rm -R {base_work_dir}\"$JOBID\"/backup_dir



	def multijob_json(self, format_dict):
		return """{jobdir_dict}
		""".format(**format_dict)