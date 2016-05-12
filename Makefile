sh:
	docker-compose run --rm bigmetadata /bin/bash

test:
	docker-compose run --rm -e PGDATABASE=test bigmetadata nosetests -s tests/

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
	  --module tasks.sphinx Catalog --force

pdf-catalog:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Catalog --format latexpdf --force

deploy-catalog: catalog pdf-catalog
	cd catalog/build/html && \
	  git add . && \
	  git commit -m 'updating catalog' && \
	  git checkout -B gh-pages && \
	  git push origin gh-pages

# do not exceed three slots available for import api
sync: sync-data
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncMetadata
#	  --parallel-scheduling --workers=3

sync-data:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncAllData
#	  --parallel-scheduling --workers=3

kill:
	docker-compose ps | grep _run_ | cut -c 1-34 | xargs docker stop

# http://stackoverflow.com/questions/2214575/passing-arguments-to-make-run#2214593
ifeq (run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

.PHONY: run catalog
#run : prog
#	@echo prog $(RUN_ARGS)
run:
	docker-compose run --rm bigmetadata luigi --local-scheduler --module tasks.$(RUN_ARGS)

dump:
	docker-compose run --rm bigmetadata luigi --module tasks.carto Dump
