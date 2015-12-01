#!/bin/bash

source env/bin/activate

if [ -f .env ]; then
  source .env
fi

PYTHONPATH=$PWD luigi --module tasks.us.census.acs AllACS --local-scheduler \
   --parallel-scheduling --workers=8 \
   > logs/luigi.log 2>logs/luigi.err
