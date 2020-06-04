#!/usr/bin/env bash
set -ex

cd /io

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=$CONF ..
make -j 4

cd /io/python

pyenv global ${PYTHON_VERSION}

python -m pip install -r requirements.txt
python -m pip install redis

python setup.py build
python setup.py install --user

python -m pytest tests
