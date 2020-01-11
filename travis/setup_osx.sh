#!/usr/bin/env bash
set -x

diskutil erasevolume HFS+ 'ram-disk' `hdiutil attach -nomount ram://6165430`
df -h

export PATH="${HOME}/.pyenv/shims/:/root/.pyenv/bin:${PATH}"

pyenv install ${PYTHON_VERSION}
pyenv global ${PYTHON_VERSION}

pip install -r python/requirements.txt
