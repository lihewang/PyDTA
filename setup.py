# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 10:47:03 2020

@author: lihe.wang

to run this file:
python setup.py build_ext --inplace
"""

import numpy
from distutils.core import setup, Extension
from Cython.Build import cythonize

setup(
    ext_modules = cythonize("heap.pyx"),
    include_dirs=[numpy.get_include()]
)