#!/bin/bash

source env/bin/activate

if [ -f .env ]; then
  source .env
fi
export PGPASSWORD=$PGPASSWORD
export PGHOST=$PGHOST
export PGUSER=$PGUSER
export PGPORT=$PGPORT
export PGOPTIONS=$PGOPTIONS
export PGDATABASE=$PGDATABASE

export PYTHONPATH=$PWD:/bigmetadata

luigi --module tasks.us.census.acs AllACS --local-scheduler \
   --parallel-scheduling --workers=8 \
   > logs/luigi.log 2>logs/luigi.err

#luigi --module tasks.us.census.acs ProcessACS --force --year 2013 --sample 1yr --local-scheduler
#luigi --module tasks.us.census.tiger Tiger --force --local-scheduler

#luigi --module tasks.us.census.tiger ProcessTiger --year 2012 --force --local-scheduler
