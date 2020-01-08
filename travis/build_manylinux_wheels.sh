#!/usr/bin/env bash
set -ex

PYBIN=/opt/python/${PYTHON_PATH}/bin

cd /io/python/

${PYBIN}/pip install -r requirements.txt

# Build
${PYBIN}/python setup.py bdist_wheel -d dist


# no native components at the moment

# Bundle external shared libraries into the wheels
#for wheel in dist/*.whl; do
#    auditwheel repair ${wheel} -w wheelhouse/
#done

# Install and test
#${PYBIN}/pip install keyviserver --no-index -f wheelhouse/

${PYBIN}/pip install keyviserver --no-index -f dist/

cd ~
#${PYBIN}/py.test /io/python/tests/
#${PYBIN}/py.test /io/python/integration-tests/
