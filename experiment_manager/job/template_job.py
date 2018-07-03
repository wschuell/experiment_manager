from .job import Job # or from outside: from experiment_manager.job import Job

import path as pathpy #not mandatory. Dependency installed via pip if you install the experiment_manager library

""" 
Folders: each function either runs in the folder local to the job, or at the top level folder. Specified just after the function name
When executing in the local job folder, a priori there is no need to be in the top level folder.
When executing in the top level folder, you can access the job folder with: j.get_path()
or even chdir in it in a clean way: with pathpy.Path(self.get_path()):
"""





class TemplateJob(Job):

	def init(self,myvariables,*args,**kwargs):#runs in top level folder / on local machine
		## !!!!! CAUTION This is init and NOT __init__ !!!!!!
		""" init as you which, set variables that you need , pack the data you need in specific files, etc. You just need to call the first line before everything, and the last line at the end"""
		self.files.append(FILES)# optional, tell here the specific files that you create and want to be transfered to the cluster
		self.clean_at_retrieval = [FILES] #optional, files to delete before batch-downloading the job folder (db journals for example, compilation artifacts, etc)

	def script(self): #runs in the folder local top the job / on cluster
		""" do what you need to do. For checkpoints, you can call check_time as showed below
		get_data will be called before, so anything you need in self.data will be there as specified in get_data"""
		self.check_time() #doing a checkpoint; optional. Checkpoints are not done every time the function is called, only when considered relevant, knowing the value of self.estimated_time

	def get_data(self): #runs in the folder local top the job / on cluster
		""" Just before running the job, how do you set up the data? Loading files for example. You should use a "data" attribute for the job object: self.data, with any type you want. For memory concerns, the data attribute is set to None when the job is serialized"""
		job.data = ...

	def save_data(self): #runs in the folder local top the job / on cluster
		"""tell how you want your data (=contents of self.data) to be saved; during a checkpoint or at the end of the job"""

	def unpack_data(self): #runs in top level folder / on local machine
		"""when you retrieved your job, after it has finished running, this function tells how to 'unpack' the data, meaning integrating it in your global database or results
		get_data is NOT called for this part, but you re free to do it by setting self.get_data_at_unpack as True in the __init___, or giving get_data_at_unpack=True as a kwarg to Job.__init__ 
		"""

	def restart(self):#runs in top level folder / on local machine #OPTIONAL
		"""when a job was missubmitted, or ended with an error, you might want to 'clean' it and make it re-runnable. Specify how here"""




	#Attributes that can be interesting (set up as kwargs of the __init__ of Job, or modified in your part of the __init__). 
	#It is not mandatory to set them, they all have default values
	self.requirements = ['-e git+https://github.com/wschuell/experiment_manager.git@origin/develop#egg=experiment_manager'] #list of requirements to be installed before running the job, via pip
	self.virtual_env = 'blabla' #virtualenv to be used for python, found in ~/virtualenvs/blabla . If you use for example ~/.venvs, make a link ln -s $HOME/.venvs $HOME/virtualenvs
	self.estimated_time = 7200 # estimated runtilme for the job, by default 2hours
	self.max_time = 48*3600 #max submission time (by default 48h). When your job is resubmitted with a greater execution time, this avoids jobs to continue running forever
	self.erase = False #whether to clean up job folders or not after job is finished and unpacked
	self.profiling= False #whether to perform a cProfile of the job while it runs or not
	self.checktime = False # whether to allow or not checkpoints
	self.get_data_at_unpack = True #do you need to call get_data before using unpack_data?





	#OPTIONAL, mainly when dealing with dependencies between jobs, or wanting to avoid running 2 times a job with the same parameters
	def re_init(self):
		""" how to reinit a job when dependencies jobs are finished"""

	def gen_depend(self):
		""" generates a list of dependency jobs that are mandatory to be executed before the actual job"""

	def re_init(self):
		""" """



	def __eq__(self,other):#OPTIONAL
		""" definition of equality, if a job is equal to another (the variable 'other') already present in the job queue it will not be added"""

	def __lt__(self,other):#OPTIONAL
		""" definition of an inequality operator, for example if you consider jobs that are similar but will be executed for more times steps. 
		For example samejob(steps=1000)>samejob(steps=500). 
		Example: if you add the second (500) when the first already exists in the queue, it will not be added"""

	def __ge__(self,other):#OPTIONAL
		""" definition of an inequality operator"""







