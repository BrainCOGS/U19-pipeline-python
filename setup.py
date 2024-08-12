#!/usr/bin/env python
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

setup(
    name='u19_pipeline',
    version='0.3.0',
    description='Datajoint schemas for Princeton U19',
    author='DataJoint',
    author_email='support@datajoint.com',
    packages=find_packages(exclude=[]),
    install_requires=requirements,
)
