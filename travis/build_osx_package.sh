#!/usr/bin/env bash
set -x

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=release -DZLIB_ROOT=/usr/local/opt/zlib ..
make -j 4 || travis_terminate 1;

export TMPDIR=/Volumes/ram-disk

cd python

pip install -r requirements.txt
pip install redis

python setup.py build
python setup.py install --user

py.test tests