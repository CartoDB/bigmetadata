SHELL = /bin/bash

###
### Tasks runners
###
ifneq (, $(findstring docker-, $$(firstword $(MAKECMDGOALS))))
  MAKE_TASK := $(shell echo $(wordlist 1,1,$(MAKECMDGOALS)) | sed "s/^docker-//g")
endif

PGSERVICE ?= postgres10
LOG_LEVEL ?= INFO
WORKERS ?= 1

###
### Tasks runners
###
ifneq (, $(findstring run, $$(firstword $(MAKECMDGOALS))))
  # From word 2 to the end is the task
  TASK := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # Remove the class name to get the module name
  # for example: echo es.cnig.AllGeometries | sed "s/^\(.*\)\..*$/\1/"
  MOD_NAME := $(shell echo $(wordlist 1,1,$(TASK)) | sed "s/^\(.*\)\..*$$/\1/")
  # ...and turn them into do-nothing targets
  $(eval $(TASK):;@:)
  $(eval $(MOD_NAME):;@:)
endif

.PHONY: run run-parallel catalog docs carto restore dataservices-api rebuild-all

run:
	python3 -m luigi $(SCHEDULER) --module tasks.$(MOD_NAME) tasks.$(TASK)

docker-run:
ifeq ($(WORKERS),1)
		PGSERVICE=$(PGSERVICE) docker-compose run -d -e LOGGING_FILE=etl_$(MOD_NAME).log bigmetadata luigi --module tasks.$(MOD_NAME) tasks.$(TASK) --log-level $(LOG_LEVEL)
else
		PGSERVICE=$(PGSERVICE) docker-compose run -d -e LOGGING_FILE=etl_$(MOD_NAME).log bigmetadata luigi --parallel-scheduling --workers $(WORKERS) --module tasks.$(MOD_NAME) tasks.$(TASK) --log-level $(LOG_LEVEL)
endif

run-parallel:
	python3 -m luigi --parallel-scheduling --workers=8 $(SCHEDULER) --module tasks.$(MOD_NAME) tasks.$(TASK)

# Run a task using docker. For example make docker-es-all
docker-%:
	docker-compose run --rm bigmetadata mkdir -p tmp/logs
	PGSERVICE=$(PGSERVICE) docker-compose run -d -e LOGGING_FILE=etl_$(MAKE_TASK).log bigmetadata make $(MAKE_TASK) SCHEDULER=$(SCHEDULER)

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
### Rebuild task
###

rebuild-all:
	./scripts/rebuild-all.sh

###
### Extensions
###
# update the observatory-extension in our DB container
# Depends on having an observatory-extension folder linked
extension:
	PGSERVICE=$(PGSERVICE) docker exec $$(docker-compose ps -q $(PGSERVICE)) sh -c 'cd /observatory-extension && make install'
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata psql -c "DROP EXTENSION IF EXISTS observatory; CREATE EXTENSION observatory WITH VERSION 'dev';"

# update dataservices-api in our DB container
# Depends on having a dataservices-api folder linked
dataservices-api:
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
extension-perftest:
	nosetests -s observatory-extension/src/python/test/perftest.py

extension-perftest-record:
	mkdir -p perftest
	OBS_RECORD_TEST=true \
	OBS_PERFTEST_DIR=perftest \
	OBS_EXTENSION_SHA=$$(cd observatory-extension && git rev-list -n 1 HEAD) \
	OBS_EXTENSION_MSG="$$(cd observatory-extension && git rev-list --pretty=oneline -n 1 HEAD)" \
	nosetests observatory-extension/src/python/test/perftest.py

extension-autotest:
	nosetests observatory-extension/src/python/test/autotest.py

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
	docker-compose run -e LOGGING_FILE=test.log --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  PGDATABASE=test nosetests -v \
	    tests/test_meta.py tests/test_util.py tests/test_carto.py \
	    tests/test_tabletasks.py tests/test_lib.py'

etl-metadatatest:
	docker-compose run -e LOGGING_FILE=test.log --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  TEST_ALL=$(ALL) TEST_MODULE=tasks.$(MODULE) \
	  PGDATABASE=test nosetests -v --with-timer \
	    tests/test_metadata.py'

travis-etl-unittest:
	./scripts/run-travis.sh \
	  'nosetests -v \
	    tests/test_meta.py tests/test_util.py tests/test_carto.py \
	    tests/test_tabletasks.py tests/test_lib.py'

travis-diff-catalog:
	git fetch origin master
	./scripts/run-travis.sh 'python3 -c "from tests.util import recreate_db; recreate_db()"'
	./scripts/run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.base_tasks tasks.base_tasks.RunDiff --compare FETCH_HEAD'
	./scripts/run-travis.sh 'ENVIRONMENT=test luigi --local-scheduler --module tasks.sphinx tasks.sphinx.Catalog --force'

travis-etl-metadatatest:
	./scripts/run-travis.sh 'nosetests -v tests/test_metadata.py'

releasetest: extension-fixtures extension-perftest-record extension-unittest extension-autotest

test-catalog:
	docker-compose run --rm bigmetadata /bin/bash -c \
	  'while : ; do pg_isready -t 1 && break; done && \
	  TEST_MODULE=tasks.$(MODULE) PGDATABASE=test nosetests -vs \
	    tests/test_catalog.py'

diff-catalog: clean-catalog
	git fetch origin master
	docker-compose run -e PGDATABASE=test -e ENVIRONMENT=test -e LOGGING_FILE="diff_catalog.log" --rm bigmetadata /bin/bash -c \
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

dump: extension
	PGSERVICE=$(PGSERVICE) docker-compose run -d -e LOGGING_FILE=etl_dump.log bigmetadata make dump-task

dump-task: test
	make run -- carto.DumpS3

docs:
	docker-compose run --rm bigmetadata /bin/bash -c 'cd docs && make html'

tiles:
	make run -- util.GenerateAllRasterTiles

meta:
	make run -- carto.OBSMetaToLocal --force

###
### Import tasks
###

### au
au-all-2011:
	make -- run au.data.XCPAllGeographiesAllTables --year 2011

au-geo-2011:
	make -- run au.geo.AllGeographies --year 2011

au-all-2016:
	make -- run au.data.XCPAllGeographiesAllTables --year 2016

au-geo-2016:
	make -- run au.geo.AllGeographies --year 2016

### br
br-all: br-geo br-census

br-census:
	make -- run br.data.CensosAllGeographiesAllTables

br-geo:
	make -- run br.geo.AllGeographies

### ca
ca-all: ca-all-2011 ca-all-2016

ca-all-2011: ca-geo-2011 ca-nhs-all-2011 ca-census-all-2011

ca-nhs-all-2011:
	make -- run ca.statcan.data.AllNHSTopics

ca-census-all-2011:
	make -- run ca.statcan.data.AllCensusTopics

ca-geo-2011:
	make -- run ca.statcan.geo.AllGeographies --year 2011

ca-all-2016: ca-geo-2016 ca-census-all-2016

ca-census-all-2016:
	make -- run ca.statcan.census2016.data.AllCensusResolutions

ca-geo-2016:
	make -- run ca.statcan.geo.AllGeographies --year 2016

### es
es-all: es-cnig es-ine

es-cnig:
	make -- run es.cnig.AllGeometries

es-ine: es-ine-phh es-ine-fyp

es-ine-phh:
	make -- run es.ine.PopulationHouseholdsHousingMeta

es-ine-fyp:
	make -- run es.ine.FiveYearPopulationMeta

### eurostat
eu-all: eu-geo eu-data

eu-geo:
	make -- run eu.geo.AllNUTSGeometries

eu-data:
	make -- run eu.eurostat.EURegionalTables

### fr
fr-all: fr-geo fr-insee fr-income

fr-geo:
	make -- run fr.geo.AllGeo

fr-insee:
	make -- run fr.insee.InseeAll

fr-income:
	make -- run fr.fr_income.IRISIncomeTables

### Geographica

geographica-us-all:
	make -- run geographica.us.csv.AllMeasurements

geographica-ca-all:
	make -- run geographica.ca.csv.AllMeasurements

geographica-au-all:
	make -- run geographica.au.csv.AllMeasurements

geographica-uk-all:
	make -- run geographica.uk.csv.AllMeasurements

### mx
mx-all: mx-geo mx-census

mx-geo:
	make -- run mx.inegi.AllGeographies

mx-census:
	make -- run mx.inegi.AllCensus

### uk
uk-all: uk-geo uk-census

uk-geo: uk-geo-cdrc uk-geo-gov uk-geo-datashare uk-geo-odl

uk-geo-cdrc:
	make -- run uk.cdrc.CDRCMetaWrapper

uk-geo-gov:
	make -- run uk.gov.GovWrapper

uk-geo-datashare:
	make -- run uk.datashare.PostcodeAreas

uk-geo-odl:
	make -- run uk.odl.ODLWrapper

uk-census: uk-census-output-areas uk-census-postcode-areas uk-census-postcode-districts uk-census-postcode-sectors uk-census-lower-super-output-areas uk-census-middle-super-output-areas

uk-census-output-areas:
	make -- run uk.census.wrapper.CensusOutputAreas

uk-census-postcode-areas:
	make -- run uk.census.wrapper.CensusPostcodeAreas

uk-census-postcode-districts:
	make -- run uk.census.wrapper.CensusPostcodeDistricts

uk-census-postcode-sectors:
	make -- run uk.census.wrapper.CensusPostcodeSectors

uk-census-lower-super-output-areas:
	make -- run uk.census.wrapper.CensusLowerSuperOutputAreas

uk-census-middle-super-output-areas:
	make -- run uk.census.wrapper.CensusMiddleSuperOutputAreas

### us
us-all: us-bls us-acs-2010 us-acs-2014 us-acs-2015 us-acs-2016 us-lodes us-spielman us-tiger-2015 us-tiger-2016 us-enviroatlas us-huc us-zillow

us-bls:
	make -- run us.bls.AllQCEW --maxtimespan 2017Q1

us-acs-2010:
	make -- run us.census.acs.ACSAll --year 2010

us-acs-2014:
	make -- run us.census.acs.ACSAll --year 2014

us-acs-2015:
	make -- run us.census.acs.ACSAll --year 2015

us-acs-2016:
	make -- run us.census.acs.ACSAll --year 2016

us-lodes:
	make -- run us.census.lodes.LODESMetaWrapper --geography block --year 2013

us-spielman:
	make -- run us.census.spielman_singleton_segments.SpielmanSingletonMetaWrapper

us-tiger-2015: us-tiger-census_tract-2015 us-tiger-county-2015 us-tiger-block_group-2015 us-tiger-congressional_district-2015 us-tiger-puma-2015 us-tiger-school_district_secondary-2015 us-tiger-state-2015 us-tiger-school_district_unified-2015 us-tiger-cbsa-2015 us-tiger-school_district_elementary-2015 us-tiger-place-2015 us-tiger-zcta5-2015 us-tiger-block-2015

us-tiger-census_tract-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography census_tract

us-tiger-county-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography county

us-tiger-block_group-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography block_group

us-tiger-congressional_district-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography congressional_district

us-tiger-puma-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography puma

us-tiger-school_district_secondary-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography school_district_secondary

us-tiger-state-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography state

us-tiger-school_district_unified-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography school_district_unified

us-tiger-cbsa-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography cbsa

us-tiger-school_district_elementary-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography school_district_elementary

us-tiger-place-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography place

us-tiger-block-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography block

us-tiger-zcta5-2015:
	make -- run us.census.tiger.SumLevel4Geo --year 2015 --geography zcta5

us-tiger-2016: us-tiger-census_tract-2016 us-tiger-county-2016 us-tiger-block_group-2016 us-tiger-congressional_district-2016 us-tiger-puma-2016 us-tiger-school_district_secondary-2016 us-tiger-state-2016 us-tiger-school_district_unified-2016 us-tiger-cbsa-2016 us-tiger-school_district_elementary-2016 us-tiger-place-2016 us-tiger-zcta5-2016 us-tiger-block-2016

us-tiger-census_tract-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography census_tract

us-tiger-county-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography county

us-tiger-block_group-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography block_group

us-tiger-congressional_district-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography congressional_district

us-tiger-puma-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography puma

us-tiger-school_district_secondary-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography school_district_secondary

us-tiger-state-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography state

us-tiger-school_district_unified-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography school_district_unified

us-tiger-cbsa-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography cbsa

us-tiger-school_district_elementary-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography school_district_elementary

us-tiger-place-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography place

us-tiger-block-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography block

us-tiger-zcta5-2016:
	make -- run us.census.tiger.SumLevel4Geo --year 2016 --geography zcta5

us-enviroatlas:
	make -- run us.epa.enviroatlas.AllTables

us-huc:
	make -- run us.epa.huc.HUC

us-dcp:
	make -- run us.ny.nyc.dcp.MapPLUTOAll

us-dob:
	make -- run us.ny.nyc.dob.PermitIssuance

us-zillow:
	make -- run us.zillow.AllZillow

### who's on first
wof-all:
	make -- run whosonfirst.AllWOF

### Mastercard

mastercard-all:
	make -- run mc.data.AllMCCountries

### Tiler tables
tiler-au-all:
	make -- docker-run tiler.xyz.AllSimpleDOXYZTables --config-file au_all.json

tiler-ca-all:
	make -- docker-run tiler.xyz.AllSimpleDOXYZTables --config-file ca_all.json

tiler-uk-all:
	make -- docker-run tiler.xyz.AllSimpleDOXYZTables --config-file uk_all.json

tiler-us-all:
	make -- docker-run tiler.xyz.AllSimpleDOXYZTables --config-file us_all.json

### Task to ease deployment of new months at production. You should use this after adding one month.
### Example of single-month load: `make -- docker-run mc.data.AllMCData --country us --month 201808`
### Once you have the month loaded, then you can use this task to create a file with the data of the month.
### Dumps a date (MM/DD/YYYY) of a country. Example: `make tiler-us-mc-increment MONTH=02/01/2018`
tiler-us-mc-increment:
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "rm -rf tmp/mc/us && mkdir -p tmp/mc/us"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_block where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/block"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_block_group where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/block_group"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_county where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/county"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_state where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/state"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_tract where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/tract"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "psql -c \"copy (select * from \\\"us.mastercard\\\".mc_zcta5 where month='${MONTH}') to stdout with (format binary)\" > tmp/mc/us/zcta5"
	PGSERVICE=$(PGSERVICE) docker-compose run --rm bigmetadata bash -c "tar cvzf tmp/mc/us.tar.gz -C tmp/mc us"

### To be run in staging/production
tiler-us-mc-increment-import:
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_block from '/tmp/us/block' with (format binary)"
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_block_group from '/tmp/us/block_group' with (format binary)"
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_county from '/tmp/us/county' with (format binary)"
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_state from '/tmp/us/state' with (format binary)"
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_tract from '/tmp/us/tract' with (format binary)"
	psql -U postgres -d gis -v ON_ERROR_STOP=1 -c "copy \"us.mastercard\".mc_zcta5 from '/tmp/us/zcta5' with (format binary)"
