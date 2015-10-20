
from. import Job

import time
import os
import shutil
import cPickle
import copy

class ExperimentJob(Job):

    def __init__(self, exp, T, *args,**kwargs):
        super(ExperimentJob, self).__init__(*args,**kwargs)
        self.data = copy.deepcopy(exp)
        self.xp_uuid = self.data.uuid
        self.T = T
        self.save()
        self.data = None

    def script(self):
        for i in range(self.data._T[-1],self.T):
            self.data.continue_exp(self.data.step)
            self.check_time()

    def get_data(self):
        with open(self.xp_uuid+'.b','r') as f:
            self.data = cPickle.loads(f.read())

    def save_data(self):
        with open(self.data.uuid+'.b','w') as f:
            f.write(cPickle.dumps(self.data))

    def unpack_data(self):
        shutil.move(self.path+'/'+self.data.uuid+'.b', self.data.uuid+'_'+self.uuid+'.b')


