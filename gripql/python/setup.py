#!/usr/bin/env python

import io
import os
import re

from setuptools import setup, find_packages


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='gripql',
    version=find_version("gripql", "__init__.py"),
    description='GRaph Integration Platform Client',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='OHSU Computational Biology',
    author_email='CompBio@ohsu.edu',
    maintainer='Adam Struck',
    maintainer_email='strucka@ohsu.edu',
    url="https://github.com/bmeg/grip/gripql/python",
    license='MIT',
    packages=find_packages(),
    python_requires='>=2.6, >=3.5, <4',
    install_requires=[
        "requests>=2.19.1"
    ],
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
)
