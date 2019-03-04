#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:copyright: (c) 2017 by Mitchell Lisle
:license: MIT, see LICENSE for more details.
"""
import os
import sys

from setuptools import setup
from setuptools.command.install import install

setup(name='termatopy',
      version='SoreScorpion',
      description='A python Termatico Package',
      url='http://github.com/termatico/termatopy',
      author='Mitchell Lisle',
      author_email='mitchell.lisle@bigdatr.com',
      packages=['termatopy'],
      license='MIT',
      install_requires=[
          'boto3',
          'pandas',
          'psycopg2',
          'numpy'
      ],
      python_requires='>=3',
      zip_safe=False)
