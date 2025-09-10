[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# GRIP Client

**gripql** is a python library for interacting with a GRIP server.

### Install

Available on [PyPI](https://pypi.org/project/gripql/).

```
pip install gripql
```

Or install the latest development version:

```
pip install "git+https://github.com/bmeg/grip.git#subdirectory=gripql/python"
```

### Getting Started

Check out the getting started guide [here](https://bmeg.github.io/grip/docs/queries/getting_started/).

## Steps to Create a New Pypi Release

1. Increment the release version. Release version can be found at the bottom of `gripql/__init__.py`
2. `export TWINE_USERNAME=` # the username to use for authentication to the repository.
3. `export TWINE_PASSWORD=` # the password to use for authentication to the repository.
4. Remove old build dir `rm -r dist/`
5. pip install dev tools `pip install -r requirements-dev.txt`
6. push a new version `twine upload dist/*`
