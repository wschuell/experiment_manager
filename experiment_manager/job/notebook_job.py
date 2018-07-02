from .job import Job
from runipy.notebook_runner import NotebookRunner
from IPython.nbformat.current import read as ipynb_read
from IPython.nbformat.current import write as ipynb_write

class NotebookJob(Job):

	def init(self, notebook_file, *args, **kwargs):
		self.requirements.append('runipy')
		self.requirements.append('ipython')
		self.get_data()

	def script(self):
		r.run_notebook()

	def get_data(self):
		with open(notebook_file, 'r') as f:
			self.data = NotebookRunner(ipynb_read(f, 'json'))

	def save_data(self):
		with open(notebook_file, 'w') as f:
			ipynb_write(self.data.nb, f, 'json')

	def unpack_data(self):
		shutil.copy(os.path.join(self.path,notebook_file), notebook_file)