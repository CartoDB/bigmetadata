#!/bin/bash

sudo mkdir -p /bigmetadata/tmp/ipython
sudo chown -R ds:ds /bigmetadata/tmp/ipython
/opt/ds/bin/jupyter-notebook --no-browser --port 8888 --ip=0.0.0.0 --notebook-dir=/bigmetadata/tmp/ipython
