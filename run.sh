#!/bin/bash

source env/bin/activate
PYTHONPATH=$PWD luigi --module tasks.us.census.acs AllACS --local-scheduler #--parallel-scheduling --workers=8
