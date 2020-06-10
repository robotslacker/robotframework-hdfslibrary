# -*- coding: UTF-8 -*-
import ast
from io import open
import re
from setuptools import setup

'''
How to build and upload this package to PyPi
    python setup.py sdist
    python setup.py bdist_wheel --universal
    twine upload dist/*
'''
_version_re = re.compile(r"ROBOT_LIBRARY_VERSION\s+=\s+(.*)")

with open("HDFSLibrary/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

setup(
    name='robotframework-hdfslibrary',
    version=version,
    description='Robot Framework keyword library for hdfs',
    keywords='robotframework hdfs',
    platforms='any',
    install_requires=['robotframework', 'hdfs'],

    author='RobotSlacker',
    author_email='184902652@qq.com',
    url='https://github.com/robotslacker/robotframework-hdfslibrary',

    zip_safe=False,
    packages=['HDFSLibrary'],
    package_data={'HDFSLibrary': []}
)
