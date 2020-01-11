#!/usr/bin/env bash
set -ex

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=release -DZLIB_ROOT=/usr/local/opt/zlib ..
make -j 4

#cd ..


#export TMPDIR=/Volumes/ram-disk

#cd python
#python setup.py bdist_wheel -d wheelhouse

# check that static linkage worked by uninstalling libraries
#brew remove zlib
#brew remove snappy

#sudo -H pip install wheelhouse/*.whl
#py.test tests
#py.test integration-tests
#cd ..
