#!/usr/bin/env bash
set -ex

pyenv global ${PYTHON_VERSION}

cd /io/python

pip install -r requirements.txt

python setup.py build
python setup.py install --user
#py.test tests
#py.test integration-tests
