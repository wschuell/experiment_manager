
branch master:
[![Build Status](https://travis-ci.org/wschuell/experiment_manager.svg?branch=master)](https://travis-ci.org/wschuell/experiment_manager)
[![codecov](https://codecov.io/gh/wschuell/experiment_manager/branch/master/graph/badge.svg)](https://codecov.io/gh/wschuell/experiment_manager)


branch develop:
[![Build Status](https://travis-ci.org/wschuell/experiment_manager.svg?branch=develop)](https://travis-ci.org/wschuell/experiment_manager)
[![codecov](https://codecov.io/gh/wschuell/experiment_manager/branch/develop/graph/badge.svg)](https://codecov.io/gh/wschuell/experiment_manager)

Tests are done for both python 2 and 3


## Experiment managing library

This framework aims at wrapping every technical aspects needed to run many similar experiments with different parameters, and was especially designed for scientific research.

It takes one line to change your execution platform from local to a cluster. Torque and Slurm already supported, new ones can easily be added.

All what is needed is to define a Job subclass, as detailed in [template_job](https://github.com/wschuell/experiment_manager/blob/develop/experiment_manager/job/template_job.py).

Explanatory notebook soon to come.

