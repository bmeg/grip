#! /usr/bin/env python

import sys
import os

from setuptools import setup, find_packages
import subprocess

from distutils import util
from distutils.spawn import find_executable
from distutils.command.build_py import build_py as _build_py

protoc = find_executable("protoc")

def generate_proto(source, require = True):
  """Invokes the Protocol Compiler to generate a _pb2.py from the given
  .proto file.  Does nothing if the output already exists and is newer than
  the input."""
  if not require and not os.path.exists(source):
    return
  output = source.replace(".proto", "_pb2.py").replace("../", "")
  if (not os.path.exists(output) or
      (os.path.exists(source) and
       os.path.getmtime(source) > os.path.getmtime(output))):
    print("Generating %s..." % output)
    if not os.path.exists(source):
      sys.stderr.write("Can't find required file: %s\n" % source)
      sys.exit(-1)
    if protoc is None:
      sys.stderr.write(
          "protoc is not installed nor found in ../src.  Please compile it "
          "or install the binary package.\n")
      sys.exit(-1)
    protoc_command = [ "python", "-m", "grpc_tools.protoc",  "-I../", "-I.", "--python_out=./gripper", "--grpc_python_out=./gripper", source ]
    if subprocess.call(protoc_command) != 0:
      sys.exit(-1)

class build_py(_build_py):
  def run(self):
      generate_proto("../gripper.proto")
      _build_py.run(self)

if __name__ == "__main__":

    setup(
        name='gripper',
        version="0.7.0",
        description='GRIP Pluggable External Resource',
        author='OHSU Computational Biology',
        author_email='CompBio@ohsu.edu',
        maintainer='Kyle Ellrott',
        maintainer_email='kellrott@gmail.com',
        url="https://github.com/bmeg/grip/gripql/python",
        license='MIT',
        packages=find_packages(),
        python_requires='>=2.7, <4',
        install_requires=[
            "requests>=2.19.1"
        ],
        cmdclass={
            'build_py': build_py,
        },
        zip_safe=True,
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Natural Language :: English',
            'License :: OSI Approved :: MIT License',
            'Topic :: Software Development :: Libraries',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7'
        ],
    )
