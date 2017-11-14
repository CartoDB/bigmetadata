SHELL = /bin/bash

###
### Tasks runners
###
ifneq (, $(filter $(firstword $(MAKECMDGOALS)), run run-local run-parallel))
  # From word 2 to the end is the task
  TASK := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # Remove the class name to get the module name
  # for example: echo es.cnig.AllGeometries | sed "s/^\(.*\)\..*$/\1/"
  MOD_NAME := $(shell echo $(wordlist 1,1,$(TASK)) | sed "s/^\(.*\)\..*$$/\1/")
  # ...and turn them into do-nothing targets
  $(eval $(TASK):;@:)
  $(eval $(MOD_NAME):;@:)
endif

.PHONY: run run-local run-parallel catalog docs carto restore dataservices-api

ifdef VIRTUAL_ENV
LUIGI := LUIGI_CONFIG_PATH=conf python3 -m luigi
else
LUIGI := docker-compose run --rm bigmetadata luigi
endif

run:
	$(LUIGI) --module tasks.$(MOD_NAME) tasks.$(TASK)

run-local:
	$(LUIGI) --local-scheduler --module tasks.$(MOD_NAME) tasks.$(TASK)

run-parallel:
	$(LUIGI) --parallel-scheduling --workers=8 --module tasks.$(MOD_NAME) tasks.$(TASK)

###
### Utils
###
sh:
	docker-compose run --rm bigmetadata /bin/bash

psql:
	docker-compose run --rm bigmetadata psql

python:
	docker-compose run --rm bigmetadata python

sh-sql:
	docker exec -it $$(docker-compose ps -q postgres) /bin/bash

py-sql:
	docker exec -it $$(docker-compose ps -q postgres) python

ps:
	docker-compose ps

stop:
	docker-compose stop

up:
	docker-compose up -d

restore:
	docker-compose run --rm -d bigmetadata pg_restore -U docker -j4 -O -x -e -d gis $(RUN_ARGS)

###
### Extensions
###
# update the observatory-extension in our DB container
# Depends on having an observatory-extension folder linked
extension:
	cd observatory-extension
	git checkout master
	git pull
	cd ..
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

###
### Tests
###
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

test: meta extension-perftest extension-autotest

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
	./run-travis.sh 'python3 -c "from tests.util import recreate_db; recreate_db()"'
	./run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.base_tasks tasks.base_tasks.RunDiff --compare FETCH_HEAD'
	./run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.sphinx tasks.sphinx.Catalog --force'

travis-etl-metadatatest:
	./run-travis.sh 'nosetests -v tests/test_metadata.py'

releasetest: extension-fixtures extension-perftest-record extension-unittest extension-autotest

test-catalog:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  TEST_MODULE=tasks.$(MODULE) PGDATABASE=test nosetests -vs \
	    tests/test_catalog.py'

diff-catalog: clean-catalog
	git fetch origin master
	docker-compose run -e PGDATABASE=test -e ENVIRONMENT=test --rm bigmetadata /bin/bash -c \
	  'python3 -c "from tests.util import recreate_db; recreate_db()" && \
	   luigi --local-scheduler --retcode-task-failed 1 --module tasks.base_tasks tasks.base_tasks.RunDiff --compare FETCH_HEAD && \
	   luigi --local-scheduler --retcode-task-failed 1 --module tasks.sphinx tasks.sphinx.Catalog'

ifeq (deps-tree,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

deps-tree:
	docker-compose run --rm bigmetadata luigi-deps-tree --module tasks.$(RUN_ARGS)

###
### Docker
###
build:
	docker build -t carto/bigmetadata:latest .

build-postgres:
	docker build -t carto/bigmetadata_postgres:latest postgres

###
### Catalog
###
clean-catalog:
	sudo rm -rf catalog/source/*/*
	# Below code eliminates everything not removed by the command above.
	# The trick here is that catalog/source is mostly ignored, but
	# we don't want to delete catalog/source/conf.py and
	# catalog/source/index.rst
	sudo git status --porcelain --ignored -- catalog/source/* \
	  | grep '^!!' \
	  | cut -c 4-1000 \
	  | xargs rm -f

catalog: clean-catalog
	make run sphinx.Catalog $${SECTION/#/--section }
	docker-compose up -d nginx
	echo Catalog accessible at http://$$(curl -s 'https://api.ipify.org')$$(docker-compose ps | grep nginx | grep -oE ':[0-9]+')/catalog/

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

###
### Tasks
###
dump: test
	make run carto.DumpS3

docs:
	docker-compose run --rm bigmetadata /bin/bash -c 'cd docs && make html'

tiles:
	make run-parallel util.GenerateAllRasterTiles

meta:
	make run -- carto.OBSMetaToLocal --force

###
### Import tasks
###

### au
au-all:
	make -- run-parallel au.data.BCPAllGeographiesAllTables --year 2011

au-geo:
	make -- run-parallel au.geo.AllGeographies --year 2011

### br
br-all: br-geo br-census

br-census:
	make -- run-parallel br.data.CensosAllGeographiesAllTables

br-geo:
	make -- run-parallel br.geo.AllGeographies

### ca
ca-all: ca-nhs-all ca-census-all

ca-nhs-all:
	make -- run-parallel ca.statcan.data.AllNHSTopics

ca-census-all:
	make -- run-parallel ca.statcan.data.AllCensusTopics

ca-geo:
	make -- run-parallel ca.statcan.geo.AllGeographies

### es
es-all: es-cnig es-ine

es-cnig:
	make -- run-parallel es.cnig.AllGeometries

es-ine: es-ine-phh es-ine-fyp

es-ine-phh:
	make -- run-parallel es.ine.PopulationHouseholdsHousingMeta

es-ine-fyp:
	make -- run-parallel es.ine.FiveYearPopulationMeta

### eurostat
eu-all: eu-geo eu-data

eu-geo:
	make -- run-parallel eu.geo.AllNUTSGeometries

eu-data:
	make -- run eu.eurostat.EURegionalTables

### fr
fr-all: fr-geo fr-insee fr-income

fr-geo:
	make -- run-parallel fr.geo.AllGeo

fr-insee:
	make -- run-parallel fr.insee.InseeAll

fr-income:
	make -- run-parallel fr.fr_income.IRISIncomeTables

### mx
mx-all: mx-geo mx-census

mx-geo:
	make -- run-parallel mx.inegi.AllGeographies

mx-census:
	make -- run-parallel mx.inegi.AllCensus

### uk
uk-all: uk-geo uk-census

uk-geo:
	make -- run-parallel uk.cdrc.CDRCMetaWrapper

uk-census:
	make -- run-parallel uk.census.wrapper.CensusWrapper

### us
us-all: us-bls us-acs us-lodes us-spielman us-tiger us-enviroatlas us-huc us-dcp us-dob us-zillow

us-bls:
	make -- run-parallel us.bls.AllQCEW

us-acs:
	make -- run-parallel us.census.acs.ACSAll

us-lodes:
	make -- run-parallel us.census.lodes.LODESMetaWrapper --geography block --year 2013

us-spielman:
	make -- run-parallel us.census.spielman_singleton_segments SpielmanSingletonMetaWrapper

us-tiger:
	make -- run-parallel us.census.tiger.AllSumLevels --year 2015

us-enviroatlas:
	make -- run-parallel us.epa.enviroatlas.AllTables

us-huc:
	make -- run-parallel us.epa.huc.HUC

us-dcp:
	make -- run-parallel us.ny.nyc.dcp.MapPLUTOAll

us-dob:
	make -- run-parallel us.ny.nyc.dob.PermitIssuance

us-zillow:
	make -- run-parallel us.zillow.AllZillow

### who's on first
wof-all:
	make -- run-parallel whosonfirst.AllWOF