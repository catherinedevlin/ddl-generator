#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='ddlgenerator',
    version='0.1.9',
    description='Generates SQL DDL that will accept Python data',
    long_description=readme + '\n\n' + history,
    author='Catherine Devlin',
    author_email='catherine.devlin@gmail.com',
    url='https://github.com/catherinedevlin/ddl-generator',
    packages=[
        'ddlgenerator',
    ],
    package_dir={'ddlgenerator': 'ddlgenerator'},
    include_package_data=True,
    install_requires=[
      "python-dateutil",
      "sqlalchemy",
      "dateutils",
      "pyyaml",
      "beautifulsoup4",
      "requests",
      "pymongo",
      "data_dispenser>=0.2.5.1",
    ],
    license="MIT",
    zip_safe=False,
    keywords='ddlgenerator',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'ddlgenerator = ddlgenerator.console:generate',
        ]
    }
)
