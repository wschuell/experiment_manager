#!/usr/bin/env python

import re
import sys

from setuptools import setup, find_packages


def version():
    with open('experiment_manager/_version.py') as f:
        return re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read()).group(1)

def requirements():
  with open('requirements.txt') as f:
    return f.readlines()

setup(name='experiment_manager',
      version=version(),
      packages=['experiment_manager'],#find_packages(),
      install_requires=[requirements()],
      author='William Schueller',
      author_email='william.schueller@gmail.com',
      description='Python Library for Simulated Experiment Management',
      url='https://github.com/wschuell/experiment_manager',
      license='GNU AFFERO GENERAL PUBLIC LICENSE Version 3',
      )
