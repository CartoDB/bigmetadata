#!/bin/bash

source env/bin/activate
python bigmetadata/tasks.py AllACS  --local-scheduler #--parallel-scheduling --workers=8
