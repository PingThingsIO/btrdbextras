#!/usr/bin/env python
# setup
# Setup script for installing btrdb bindings
#
# Author:   PingThings
# Created:  Mon Jan 07 14:45:32 2019 -0500
#
# For license information, see LICENSE.txt
# ID: setup.py [] allen@pingthings.io $

"""
Setup script for installing btrdb bindings.
"""

##########################################################################
## Imports
##########################################################################

import os
import codecs

from setuptools import setup
from setuptools import find_packages

from btrdbextras import __version__

##########################################################################
## Package Information
##########################################################################

## Basic information
NAME         = "btrdbextras"
DESCRIPTION  = "Enhancements additional features to interact with the Berkeley Tree Database"
AUTHOR       = "Allen Leis"
EMAIL        = "allen@pingthings.io"
MAINTAINER   = "Allen Leis"
LICENSE      = "BSD-3-Clause"
REPOSITORY   = "https://github.com/PingThingsIO/btrdbextras"
PACKAGE      = "btrdb"
URL          = "https://btrdbextras.readthedocs.io/en/latest/"
DOCS_URL     = "https://btrdbextras.readthedocs.io/en/latest/"

## Define the keywords
KEYWORDS     = ('btrdb', 'timeseries', 'database')

## Define the classifiers
## See https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS  = (
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
)

## Important Paths
PROJECT      = os.path.abspath(os.path.dirname(__file__))
REQUIRE_PATH = "requirements.txt"
VERSION_PATH = os.path.join(PACKAGE, "version.py")
PKG_DESCRIBE = "DESCRIPTION.md"

## Directories to ignore in find_packages
EXCLUDES     = (
    "tests", "docs",
)

##########################################################################
## Helper Functions
##########################################################################

def read(*parts):
    """
    Assume UTF-8 encoding and return the contents of the file located at the
    absolute path from the REPOSITORY joined with *parts.
    """
    with codecs.open(os.path.join(PROJECT, *parts), 'rb', 'utf-8') as f:
        return f.read()


def get_requires(path=REQUIRE_PATH):
    """
    Yields a generator of requirements as defined by the REQUIRE_PATH which
    should point to a requirements.txt output by `pip freeze`.
    """
    for line in read(path).splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            yield line


def get_description_type(path=PKG_DESCRIBE):
    """
    Returns the long_description_content_type based on the extension of the
    package describe path (e.g. .txt, .rst, or .md).
    """
    _, ext = os.path.splitext(path)
    return {
        ".rst": "text/x-rst",
        ".txt": "text/plain",
        ".md": "text/markdown",
    }[ext]


##########################################################################
## Define the configuration
##########################################################################

config = {
    "name": NAME,
    "version": __version__,
    "description": DESCRIPTION,
    "long_description": read(PKG_DESCRIBE),
    "long_description_content_type": get_description_type(PKG_DESCRIBE),
    "classifiers": list(CLASSIFIERS),
    "keywords": list(KEYWORDS),
    "license": LICENSE,
    "author": AUTHOR,
    "author_email": EMAIL,
    "url": URL,
    "maintainer": MAINTAINER,
    "maintainer_email": EMAIL,
    "project_urls": {
        "Documentation": DOCS_URL,
        "Download": "{}/tarball/v{}".format(REPOSITORY, __version__),
        "Source": REPOSITORY,
        "Tracker": "{}/issues".format(REPOSITORY),
    },
    "download_url": "{}/tarball/v{}".format(REPOSITORY, __version__),
    "packages": find_packages(where=PROJECT, exclude=EXCLUDES),
    "package_data": {
        "btrdb": ["grpcinterface/btrdb.proto"],
    },
    "zip_safe": False,
    "entry_points": {
        "console_scripts": [],
    },
    "install_requires": list(get_requires()),
    "python_requires": ">=3.6, <4",
    "setup_requires":["pytest-runner"],
    "tests_require":["pytest"],
}


##########################################################################
## Run setup script
##########################################################################

if __name__ == '__main__':
    setup(**config)
