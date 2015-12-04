#!/bin/bash

source env/bin/activate

if [ -f .env ]; then
  source .env
fi

export PYTHONPATH=$PWD:/bigmetadata
luigi --module tasks.us.census.acs AllACS --local-scheduler \
   --parallel-scheduling --workers=8 \
   > logs/luigi.log 2>logs/luigi.err

#luigi --module tasks.us.census.acs ProcessACS --force --year 2010 --sample 5yr --local-scheduler
#luigi --module tasks.us.census.tiger Tiger --force --local-scheduler
