#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

VERSION = "0.1-git"

from setuptools import setup, find_packages


setup(
    name="Pypet",
    version=VERSION,
    description="A mini rolap library",
    long_description=__doc__,
    author="Kozea",
    license="AGPL",
    author_email="ronan.dunklau@kozea.fr",
    install_requires=['sqlalchemy>=0.9', 'psycopg2'],
    platforms="Any",
    packages=find_packages(
        exclude=["*._test", "*._test.*", "test.*", "test"]),
    provides=["pypet"])
