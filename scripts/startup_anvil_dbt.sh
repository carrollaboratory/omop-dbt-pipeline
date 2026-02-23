#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Print commands and their arguments as they are executed

# Update and install required packages for building Python
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    wget \
    curl \
    git \
    libsqlite3-dev

# Install Python 3.12
PYTHON_VERSION=3.12.0
PYTHON_SRC_DIR=/opt/python-$PYTHON_VERSION

# Download and extract Python source code
cd /tmp
wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
tar -xzf Python-$PYTHON_VERSION.tgz

# Build and install Python 3.12
cd Python-$PYTHON_VERSION
./configure --enable-optimizations
make -j$(nproc)
sudo make altinstall  # Use altinstall to avoid overwriting the default 'python3'

# Verify Python 3.12 installation
/usr/local/bin/python3.12 --version

# Set up Python 3.12 virtual environment
PYTHON_BIN=/usr/local/bin/python3.12
VENV_DIR=/home/jupyter/venv-python3.12

$PYTHON_BIN -m venv $VENV_DIR
source $VENV_DIR/bin/activate

# Install terra env requirements
pip install --upgrade pip
pip install ipykernel
pip install pathlib

# Install dbt pipeline requirements
pip install -r https://raw.githubusercontent.com/NIH-NCPI/anvil_dbt_project/main/requirements.txt

# Install venv as kernel
python -m ipykernel install --user --name=venv-python3.12 --display-name "Python 3.12 (venv)"

echo "Python 3.12 installed successfully!"
