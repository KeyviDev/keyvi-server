#!/usr/bin/env bash
set -ex

pyenv global 3.5.7
pip install cpplint

cd /io
./src/check-style.sh
