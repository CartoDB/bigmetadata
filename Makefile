sh:
	docker-compose run bigmetadata /bin/bash

test:
	docker-compose run bigmetadata nosetests -s tests/

python:
	docker-compose run bigmetadata python

build:
	docker-compose build

psql:
	docker-compose run bigmetadata psql

acs:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs AllACS \
	  --parallel-scheduling --workers=8 --force

acs-carto:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.acs ExtractAllACS \
	  --parallel-scheduling --workers=8 --force \
	  --year 2013 --sample 5yr

tiger:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger Tiger --force

tiger-carto:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger ExtractAllTiger \
	  --parallel-scheduling --workers=8 --force \
	  --year 2013

index:
	docker-compose run bigmetadata luigi \
	  --module tasks.elastic Index

sphinx:
	docker-compose run bigmetadata luigi \
	  --module tasks.sphinx Sphinx

sphinx-deploy:
	cd catalog/build/html && \
	  git add . && \
	  git commit -m 'updating catalog' && \
	  git push origin gh-pages
