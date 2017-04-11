sh:
	docker-compose run --rm bigmetadata /bin/bash

extension-perftest: extension
	docker-compose run --rm bigmetadata nosetests -s observatory-extension/src/python/test/perftest.py

extension-perftest-record: extension
	mkdir -p perftest
	docker-compose run --rm \
	  -e OBS_RECORD_TEST=true \
	  -e OBS_PERFTEST_DIR=perftest \
	  -e OBS_EXTENSION_SHA=$$(cd observatory-extension && git rev-list -n 1 HEAD) \
	  -e OBS_EXTENSION_MSG="$$(cd observatory-extension && git rev-list --pretty=oneline -n 1 HEAD)" \
	  bigmetadata \
	  nosetests observatory-extension/src/python/test/perftest.py

extension-autotest: extension
	docker-compose run --rm bigmetadata nosetests observatory-extension/src/python/test/autotest.py

test: extension-perftest extension-autotest

python:
	docker-compose run --rm bigmetadata python

build:
	docker-compose build

build-postgres:
	docker build -t recessionporn/bigmetadata_postgres:latest postgres

psql:
	docker-compose run --rm bigmetadata psql

acs:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.us.census.acs ExtractAll \
	  --year 2015 --sample 5yr
	  --parallel-scheduling --workers=8

tiger:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.us.census.tiger AllSumLevels --year 2015
	  --parallel-scheduling --workers=8

au-data:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.au.data BCPAllGeographiesAllTables --year 2011 \
	  --parallel-scheduling --workers=8

au-geo:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.au.geo AllGeographies --year 2011 \
	  --parallel-scheduling --workers=8

catalog-noimage:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --force \
	  --parallel-scheduling --workers=3

catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --force --images \
	  --parallel-scheduling --workers=3

pdf-catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --format pdf --force

md-catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --format markdown --force --images \
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

ifeq (run-parallel,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

.PHONY: run run-parallel catalog docs carto restore dataservices-api

run:
	docker-compose run --rm bigmetadata luigi --local-scheduler --module tasks.$(RUN_ARGS)

run-parallel:
	docker-compose run --rm bigmetadata luigi --parallel-scheduling --workers=8 --module tasks.$(RUN_ARGS)

dump: test
	docker-compose run --rm bigmetadata luigi --module tasks.carto DumpS3

# update the observatory-extension in our DB container
# Depends on having an observatory-extension folder linked
extension:
	docker exec $$(docker-compose ps -q postgres) sh -c 'cd observatory-extension && make install'
	docker-compose run --rm bigmetadata psql -c "DROP EXTENSION IF EXISTS observatory; CREATE EXTENSION observatory WITH VERSION 'dev';"

# update dataservices-api in our DB container
# Depends on having a dataservices-api folder linked
dataservices-api: extension
	docker exec $$(docker-compose ps -q postgres) sh -c ' \
	  cd /cartodb-postgresql && make install && \
	  cd /data-services/geocoder/extension && make install && \
	  cd /dataservices-api/client && make install && \
	  cd /dataservices-api/server/extension && make install && \
	  cd /dataservices-api/server/lib/python/cartodb_services && \
	  pip install -r requirements.txt && pip install --upgrade .'
	docker-compose run --rm bigmetadata psql -f /bigmetadata/postgres/dataservices_config.sql
	docker exec $$(docker-compose ps -q redis) sh -c \
	  "$$(cat postgres/dataservices_config.redis)"

## in redis:


sh-sql:
	docker exec -it $$(docker-compose ps -q postgres) /bin/bash

py-sql:
	docker exec -it $$(docker-compose ps -q postgres) python

# Regenerate fixtures for the extension
extension-fixtures:
	docker-compose run --rm bigmetadata \
	  python observatory-extension/scripts/generate_fixtures.py

extension-unittest:
	docker exec -it \
	  $$(docker-compose ps -q postgres) \
	  /bin/bash -c "cd observatory-extension \
	                && chmod -R a+w src/pg/test/results \
	                && make install \
	                && su postgres -c 'make test'"

dataservices-api-client-unittest:
	docker exec -it \
	  $$(docker-compose ps -q postgres) \
	  /bin/bash -c "cd dataservices-api/client \
	                && chmod -R a+w test \
	                && make install \
	                && su postgres -c 'PGUSER=postgres make installcheck'" || :
	test $$(grep '^[-+] ' dataservices-api/client/test/regression.diffs | grep -Ev '(CONTEXT|PL/pgSQL)' | tee dataservices-api/client/test/regression.diffs | wc -l) = 0

dataservices-api-server-unittest:
	docker exec -it \
	  $$(docker-compose ps -q postgres) \
	  /bin/bash -c "cd dataservices-api/server/extension \
	                && chmod -R a+w test \
	                && make install \
	                && su postgres -c 'PGUSER=postgres make installcheck'" || :

dataservices-api-unittest: dataservices-api-server-unittest dataservices-api-client-unittest

etl-unittest:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  PGDATABASE=test nosetests -v \
	    tests/test_meta.py tests/test_util.py tests/test_carto.py \
	    tests/test_tabletasks.py'

etl-metadatatest:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  TEST_ALL=$(ALL) TEST_MODULE=tasks.$(MODULE) \
	  PGDATABASE=test nosetests -v --with-timer \
	    tests/test_metadata.py'

travis-etl-unittest:
	./run-travis.sh \
	  'nosetests -v \
	    tests/test_meta.py tests/test_util.py tests/test_carto.py \
	    tests/test_tabletasks.py'

travis-diff-catalog:
	git fetch origin master
	./run-travis.sh 'python -c "from tests.util import recreate_db; recreate_db()"'
	./run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.util RunDiff --compare FETCH_HEAD'
	./run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.sphinx Catalog'

travis-etl-metadatatest:
	./run-travis.sh 'nosetests -v tests/test_metadata.py'

restore:
	docker-compose run --rm -d bigmetadata pg_restore -U docker -j4 -O -x -e -d gis $(RUN_ARGS)

docs:
	docker-compose run --rm bigmetadata /bin/bash -c 'cd docs && make html'

tiles:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.util GenerateAllRasterTiles \
	  --parallel-scheduling --workers=5

eurostat-data:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.eu.eurostat_bulkdownload EURegionalTables \
	  --parallel-scheduling --workers=2

ps:
	docker-compose ps

stop:
	docker-compose stop

up:
	docker-compose up -d

meta:
	docker-compose run --rm bigmetadata luigi\
	  --module tasks.carto OBSMetaToLocal

releasetest: extension-fixtures extension-perftest-record extension-unittest extension-autotest

test-catalog:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  TEST_MODULE=tasks.$(MODULE) PGDATABASE=test nosetests -vs \
	    tests/test_catalog.py'

diff-catalog:
	git fetch origin master
	docker-compose run -e PGDATABASE=test -e ENVIRONMENT=test --rm bigmetadata /bin/bash -c \
	  'python -c "from tests.util import recreate_db; recreate_db()" && \
	   luigi --local-scheduler --retcode-task-failed 1 --module tasks.util RunDiff --compare FETCH_HEAD && \
	   luigi --local-scheduler --retcode-task-failed 1 --module tasks.sphinx Catalog --force'

#restore:
#	docker exec -it bigmetadata_postgres_1 /bin/bash -c "export PGUSER=docker && export PGPASSWORD=docker && export PGHOST=localhost && pg_restore -j4 -O -d gis -x -e /bigmetadata/tmp/carto/Dump_2016_11_16_c14c5977ac.dump >/bigmetadata/tmp/restore.log 2>&1"

