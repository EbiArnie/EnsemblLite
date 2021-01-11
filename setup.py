#!/usr/bin/env python
import os
import re
import subprocess
import sys

from setuptools import Command, setup
from setuptools.extension import Extension

__author__ = "Gavin Huttley"
__copyright__ = "Copyright 2016-, The Cogent Project"
__contributors__ = ["Gavin Huttley", "Hua Ying"]
__license__ = "BSD"
__version__ = "3.0a1"
__maintainer__ = "Gavin Huttley"
__email__ = "Gavin.Huttley@anu.edu.au"
__status__ = "Production"

# Check Python version, no point installing if unsupported version inplace
if sys.version_info < (3, 6):
    py_version = ".".join([str(n) for n in sys.version_info])
    raise RuntimeError(
        "Python-3.6 or greater is required, Python-%s used." % py_version
    )


short_description = "Ensembl DB"

# This ends up displayed by the installer
long_description = (
    """ensembldb3
A toolkit for querying the Ensembl MySQL databases.
Version %s.
"""
    % __version__
)

setup(
    name="ensembldb3",
    version=__version__,
    url="https://github.com/cogent3/ensembldb3",
    author="Gavin Huttley, Hua Ying",
    author_email="gavin.huttley@anu.edu.au",
    description=short_description,
    long_description=long_description,
    platforms=["any"],
    license=["BSD"],
    keywords=["biology", "genomics", "bioinformatics"],
    classifiers=[
        "Development status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: OS Independent",
    ],
    packages=["ensembldb3"],
    dependency_links=["ssh://git@github.com:cogent3/cogent3.git"],
    install_requires=["numpy", "cogent3", "click", "PyMySQL", "sqlalchemy"],
    entry_points={
        "console_scripts": [
            "ensembldb3=ensembldb3.admin:main",
        ],
    },
    package_dir={"ensembldb3": "ensembldb3"},
    package_data={
        "ensembldb3": [
            "data/ensembldb_download.cfg",
            "data/mysql.cfg",
            "data/species.tsv",
        ]
    },
)
