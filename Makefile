sh:
	docker-compose run bigmetadata /bin/bash

test:
	docker-compose run -e PGDATABASE=test bigmetadata nosetests -s tests/

python:
	docker-compose run bigmetadata python

build:
	docker-compose build

psql:
	docker-compose run bigmetadata psql

acs:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs ExtractAll \
	  --parallel-scheduling --workers=8 --year 2013 --sample 5yr

tiger:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger AllSumLevels \
	  --parallel-scheduling --workers=8

sphinx:
	docker-compose run bigmetadata luigi \
	  --module tasks.sphinx Sphinx --force

sphinx-deploy:
	cd catalog/build/html && \
	  git add . && \
	  git commit -m 'updating catalog' && \
	  git push origin gh-pages

# do not exceed three slots available for import api
sync-meta:
	docker-compose run bigmetadata luigi \
	  --module tasks.carto SyncMetadata \
	  --parallel-scheduling --workers=3

sync-data:
	docker-compose run bigmetadata luigi \
	  --module tasks.carto SyncData

sync: sync-meta sync-data

kill:
	docker-compose ps | grep _run_ | cut -c 1-34 | xargs docker stop
