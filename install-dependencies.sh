#!/usr/bin/env bash

# ensure required python modules are installed
sudo apt-get install python3-distutils python3-apt

# ensure python is set to python3
sudo update-alternatives --install /usr/bin/python python /usr/bin/python2 1
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 2

# install poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
