#!/usr/bin/env bash
set -ex

cd /io

# TODO: move into the docker image
apt-get install libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=$CONF ..
make -j 4
