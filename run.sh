#!/bin/bash

source env/bin/activate
PGDATABASE=census PGUSER=$(whoami) PYTHONPATH=$PWD luigi --module tasks.us.census.acs AllACS --local-scheduler \
  #--parallel-scheduling --workers=8
