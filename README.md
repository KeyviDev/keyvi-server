# keyvi-server

A key value store powered by keyvi. In a nutshell this project adds the network stack to the [keyvi](https://github.com/KeyviDev/keyvi) library utilizing [brpc](https://github.com/apache/incubator-brpc).

## Builld Instructions

### Pre-Requisites

#### Linux

In addition to a working gcc build environment, install the following libraries, e.g. using apt: boost (dev packages, at least version 1.54), snappy, zlib, cmake.

For example on Ubuntu should install all the dependencies you need:

    apt-get install cmake cython g++ libboost-all-dev libsnappy-dev libzzip-dev python-stdeb zlib1g-dev libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev

#### MAC

In addition to a working build setup (Xcode) install the following libraries using Homebrew:

    brew install boost
    brew install snappy
    brew install lzlib
    brew install cmake
    brew install openssl
    brew install gnu-getopt
    brew install coreutils
    brew install gflags
    brew install protobuf
    brew install leveldb

Now you should be able to compile as explained above.

## Build

Use `cmake` to build keyvi executables along with unit tests.

Example:

    mkdir build_dir_<BUILD_TYPE>
    cd build_dir_<BUILD_TYPE>
    cmake -DCMAKE_BUILD_TYPE=<BUILD_TYPE> ..
    make

`<BUILD_TYPE>` can be `release`, `debug`, `coverage` or any other available by default in `cmake`

## Use

After compiling you can start keyviserver directly from the build directory
