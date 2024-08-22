"""Setup for checksig package"""
import sys
from distutils.errors import CompileError
from subprocess import call

from setuptools import Extension, setup, find_packages
from setuptools.command.build_ext import build_ext
 

class build_go_ext(build_ext):
    """Custom command to build extension from Go source files"""
    def build_extension(self, ext):
        ext_path = self.get_ext_fullpath(ext.name)
        cmd = ['go', 'build', '-buildmode=c-shared', '-o', ext_path]
        cmd += ext.sources
        out = call(cmd)
        if out != 0:
            raise CompileError('Go build failed')

setup(
    name='pygrip',
    version='0.8.0',
    packages=find_packages(include=['pygrip']), 
    #py_modules=['pygrip'],
    ext_modules=[
        Extension('pygrip/_pygrip', ['./pygrip/wrapper.go'])
    ],
    cmdclass={'build_ext': build_go_ext},
    install_requires=[
        "gripql>=0.8.0"
    ],
    zip_safe=False,
)