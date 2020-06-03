#!/usr/bin/env bash
set -ex

cd /io

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=$CONF ..
make -j 4

cd /io/python

pyenv global ${PYTHON_VERSION}

pip install -r requirements.txt

python setup.py build
python setup.py install --user

py.test tests
