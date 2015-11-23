#!/bin/bash

source env/bin/activate
python bigmetadata/tasks.py ACSDownloadTask  --local-scheduler --parallel-scheduling --workers=8
