__author__ = 'yong8'

from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext


setup(
    ext_modules = cythonize("main_cython.pyx"), requires=['Cython']
)
