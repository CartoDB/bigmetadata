sh:
	docker-compose run bigmetadata /bin/bash

test:
	docker-compose run bigmetadata nosetests tests/

python:
	docker-compose run bigmetadata python

build:
	docker-compose build bigmetadata

acs:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs ProcessACS \
	  --force --year 2010 --sample 1yr --local-scheduler
