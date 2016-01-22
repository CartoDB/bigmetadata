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

tiger:
	docker-compose run bigmetadata luigi \
	  --module tasks.us.census.tiger Tiger --force

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
