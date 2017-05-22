#!/usr/bin/env python

from distutils.core import setup

install_requires = ['IP2Location>=8.0.0', 'numpy>=1.12',]

setup(name='I8 Net-Utils',
      version='0.1',
      description='Different network utilities from the TUM I8 chair of network architectures and '
                  'services',
      author='TUM I8',
      author_email='scheitle@net.in.tum.de',
      url='https://github.com/tumi8/net-utils',
      packages=['netutils'],
      install_requires=install_requires,
     )
