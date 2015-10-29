
from. import Job

import time
import os
import shutil
import cPickle
import copy
import json
import kidlearn_lib as klib
from .classic_job import IteratedJob

class KidlearnJob(IteratedJob):

    def script(self):
        cost = {}
        nb_step = self.data.nb_step
        for key,val in self.data.groups.items():
            yolo = copy.deepcopy(val)
            yolo[0].run(nb_step)
            cost[key] = copy.deepcopy(yolo[0].calcul_cost())
            del yolo
        jstr = json.dumps(cost)

        with open("cost.json","w") as f:
            f.write(jstr)

        self.data = None

    def unpack_data(self):
        with open(os.path.join(self.path,"cost.json"),"r") as f:
            cost = json.loads(f.read())

        all_cost_file_name = "all_cost_{}.json".format(self.descr)
        if os.path.isfile(all_cost_file_name):
            with open(all_cost_file_name,"r") as f:
                all_cost = json.loads(f.read())
        else:
            all_cost = {}

        with open(all_cost_file_name,"w") as f:
            all_cost.update(cost)
            f.write(json.dumps(all_cost))






