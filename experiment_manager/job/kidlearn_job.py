
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

    def gen_xp_to_optimize(zpdes_confs,xp_conf,ref_xp="optimize",nb_stud=1000,nb_step=100, base_path_to_save="experimentation/data/"):
        stud = k_lib.student.KTstudent(params=xp_conf["stud"])

        wkgs = {}
        for ref,conf in zpdes_confs.items():
            zpdes = k_lib.seq_manager.ZpdesHssbg(params=conf)
            wss = []
            for k in range(nb_stud):
                wss.append(k_lib.experimentation.WorkingSession(student=copy.deepcopy(stud), seq_manager=copy.deepcopy(zpdes)))
            wkgs["zpdes_{}".format(ref)] = [k_lib.experimentation.WorkingGroup(WorkingSessions=wss)]

        xp = k_lib.experimentation.Experiment(WorkingGroups=wkgs,
                            ref_expe=ref_xp,
                            path_to_save=base_path_to_save,
                            seq_manager_list=wkgs.keys(),
                            nb_step=nb_step,
                            population={"nb_students" : nb_stud, 
                                        "model" : "KT_student"})
        return xp


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






