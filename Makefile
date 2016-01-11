sh:
	docker-compose run bigmetadata /bin/bash

test:
	docker-compose run bigmetadata nosetests -s tests/

python:
	docker-compose run bigmetadata python

build:
	docker-compose build bigmetadata

psql:
	docker-compose run bigmetadata psql

acs:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs AllACS \
	  --parallel-scheduling --workers=8 --force

load:
	docker-compose run bigmetadata python bigmetadata/load.py
