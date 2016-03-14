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
	  --module tasks.us.census.acs AllACS \
	  --parallel-scheduling --workers=8

acs-carto:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs ExtractAllACS \
	  --parallel-scheduling --workers=8 \
	  --year 2013 --sample 5yr --clipped

tiger:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger Tiger

tiger-carto:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger ExtractAllTiger \
	  --parallel-scheduling --workers=8 \
	  --year 2013

sphinx:
	docker-compose run bigmetadata luigi \
	  --module tasks.sphinx Sphinx --force

sphinx-deploy:
	cd catalog/build/html && \
	  git add . && \
	  git commit -m 'updating catalog' && \
	  git push origin gh-pages

kill:
	docker-compose ps | grep _run_ | cut -c 1-34 | xargs docker stop
