
from. import Job

import time
import os
import shutil
import cPickle
import copy
import json
import kidlearn_lib as k_lib
from .classic_job import IteratedJob


class KidlearnJob(IteratedJob):

    def script(self):
        xp = self.gen_multi_zpdes(self.data)

        self.out_files = ["{}.dat".format(xp.uuid)]

        cost = {}
        nb_step = xp.nb_step
        for key, group in xp.groups.items():
            cost[key] = []
            for subgroup in group:
                yolo = copy.deepcopy(subgroup)
                yolo.run(nb_step)
                cost[key].append(copy.deepcopy(yolo.calcul_cost()))
                del yolo
        jstr = json.dumps(cost)

        with open("cost.json", "w") as f:
            f.write(jstr)

        with open("data_job.json", "w") as f:
            f.write(json.dumps(self.data))

    def gen_multi_zpdes(self, xp_conf, ref_xp="optimize", base_path_to_save="experimentation/data/"):
        stud_confs = xp_conf["stud_confs"]
        zpdes_confs = xp_conf["zpdes_conf"]
        nb_stud = len(stud_confs)
        nb_step = xp_conf["nb_steps"]

        wkgs = {}
        for ref, confs in zpdes_confs.items():
            wkgs["zpdes_{}".format(ref)] = []
            for conf in confs:
                zpdes = k_lib.seq_manager.ZpdesHssbg(params=conf)

                wss = []
                for k in range(nb_stud):
                    stud = k_lib.student.KTstudent(params=stud_confs[k])
                    wss.append(k_lib.experimentation.WorkingSession(student=stud, seq_manager=copy.deepcopy(zpdes)))
                wkgs["zpdes_{}".format(ref)].append(k_lib.experimentation.WorkingGroup(WorkingSessions=wss))

        xp = k_lib.experimentation.Experiment(WorkingGroups=wkgs,
                                              ref_expe=ref_xp,
                                              path_to_save=base_path_to_save,
                                              seq_manager_list=wkgs.keys(),
                                              nb_step=nb_step,
                                              population={"nb_students": nb_stud,
                                                          "model": "KT_student"})
        return xp

    def gen_xp_conf_multi_grpah(self):

        return

    def unpack_data(self):
        with open(os.path.join(self.path, "cost.json"), "r") as f:
            cost = json.loads(f.read())

        jq_path = self.jq_path  # "jq_data/jq_{}/".format(self.descr)
        all_cost_file_path = "{}/all_cost_{}.json".format(jq_path, self.descr)
        if os.path.isfile(all_cost_file_path):
            with open(all_cost_file_path, "r") as f:
                all_cost = json.loads(f.read())
        else:
            k_lib.config.datafile.create_directories([jq_path])
            all_cost = {}

        with open(all_cost_file_path, "w") as f:
            all_cost.update(cost)
            f.write(json.dumps(all_cost))
