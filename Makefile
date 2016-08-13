sh:
	docker-compose run --rm bigmetadata /bin/bash

perftest: extension
	docker-compose run --rm bigmetadata nosetests -s observatory-extension/src/python/test/perftest.py

autotest: extension
	docker-compose run --rm bigmetadata nosetests observatory-extension/src/python/test/autotest.py

test: perftest autotest

python:
	docker-compose run --rm bigmetadata python

build:
	docker-compose build

psql:
	docker-compose run --rm bigmetadata psql

acs:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.us.census.acs ExtractAll \
	  --year 2014 --sample 5yr
#	  --parallel-scheduling --workers=8

tiger:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.us.census.tiger AllSumLevels --year 2014
#	  --parallel-scheduling --workers=8

catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --force \
	  --parallel-scheduling --workers=3

pdf-catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --format pdf --force

md-catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --format markdown --force \
	  --local-scheduler

deploy-pdf-catalog:
	docker-compose run --rm bigmetadata luigi \
	    --module tasks.sphinx PDFCatalogToS3

deploy-html-catalog:
	cd catalog/build/html && \
	sudo chown -R ubuntu:ubuntu . && \
	touch .nojekyll && \
	git init && \
	git checkout -B gh-pages && \
	git add . && \
	git commit -m "updating catalog" && \
	(git remote add origin git@github.com:cartodb/bigmetadata.git || : ) && \
	git push -f origin gh-pages

deploy-md-catalog:
	cd catalog/build/markdown && \
	sudo chown -R ubuntu:ubuntu . && \
	touch .nojekyll && \
	git init && \
	git checkout -B markdown-catalog && \
	git add . && \
	git commit -m "updating catalog" && \
	(git remote add origin git@github.com:cartodb/bigmetadata.git || : ) && \
	git push -f origin markdown-catalog

deploy-catalog: deploy-pdf-catalog deploy-html-catalog deploy-md-catalog

# do not exceed three slots available for import api
sync: sync-data sync-meta

sync-data:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncAllData \
	  --parallel-scheduling --workers=3

sync-meta:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncMetadata \
	  --parallel-scheduling --workers=3

kill:
	docker-compose ps | grep _run_ | cut -c 1-34 | xargs docker stop

# http://stackoverflow.com/questions/2214575/passing-arguments-to-make-run#2214593
ifeq (run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

.PHONY: run catalog docs carto

run:
	docker-compose run --rm bigmetadata luigi --local-scheduler --module tasks.$(RUN_ARGS)

dump: test
	docker-compose run --rm bigmetadata luigi --module tasks.carto DumpS3

# update the observatory-extension in our DB container
# Depends on having an observatory-extension folder linked
extension:
	docker exec $$(docker-compose ps -q postgres) sh -c 'cd observatory-extension && make install'
	docker-compose run --rm bigmetadata psql -c "DROP EXTENSION IF EXISTS observatory; CREATE EXTENSION observatory WITH VERSION 'dev';"

sh-sql:
	docker exec -it $$(docker-compose ps -q postgres) /bin/bash

api-unittest:
	docker exec -it \
	  $$(docker-compose ps -q postgres) \
	  /bin/bash -c "cd observatory-extension \
	                && chmod -R a+w src/pg/test/results \
	                && make install \
	                && su postgres -c 'make test'"

etl-unittest:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  PGDATABASE=test nosetests -v tests/test_columntasks.py tests/test_tabletasks.py'

restore:
	docker-compose run --rm -d bigmetadata pg_restore -U docker -j4 -O -x -e -d gis $(RUN_ARGS)

docs:
	docker-compose run --rm bigmetadata /bin/bash -c 'cd docs && make html'
