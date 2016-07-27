#!/bin/bash

mkdir -p /bigmetadata/tmp/ipython
/opt/ds/bin/jupyter-notebook --no-browser --port 8888 --ip=0.0.0.0 --notebook-dir=/bigmetadata/tmp/ipython
