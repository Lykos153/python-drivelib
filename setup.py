#!/usr/bin/env python3

# annexremote Setup
# Copyright (C) 2017 Silvio Ankermann
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of version 3 of the GNU General Public License as published by
# the Free Software Foundation
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


from setuptools import setup, find_packages
from codecs import open

import versioneer

def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='drivelib',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Easy access to the most common Google Drive API calls',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/Lykos153/python-drivelib',
    author='Silvio Ankermann',
    author_email='silvio@booq.org',
    license='GPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='Google Drive GoogleDrive API',
    packages=['drivelib'],

    extras_require={
        'test': ['coverage'],
    },
    install_requires=[
        'google-api-python-client',
        'google-auth-oauthlib',
        'google-auth-httplib2',
        'google-auth',
        'oauth2client',
      ],
)
