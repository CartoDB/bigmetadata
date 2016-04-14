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
	  --year 2013 --sample 5yr
#	  --parallel-scheduling --workers=8

tiger:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.us.census.tiger AllSumLevels
#	  --parallel-scheduling --workers=8

sphinx:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Sphinx --force

sphinx-deploy:
	cd catalog/build/html && \
	  git add . && \
	  git commit -m 'updating catalog' && \
	  git checkout -B gh-pages && \
	  git push origin gh-pages

sphinx-pdf:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.sphinx Sphinx --format latexpdf --force

# do not exceed three slots available for import api
sync-meta:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncMetadata
#	  --parallel-scheduling --workers=3

sync-data:
	docker-compose run --rm bigmetadata luigi \
	  --module tasks.carto SyncAllData
#	  --parallel-scheduling --workers=3

sync: sync-meta sync-data

kill:
	docker-compose ps | grep _run_ | cut -c 1-34 | xargs docker stop

# http://stackoverflow.com/questions/2214575/passing-arguments-to-make-run#2214593
ifeq (run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

.PHONY: run
#run : prog
#	@echo prog $(RUN_ARGS)
run:
	docker-compose run --rm bigmetadata luigi --local-scheduler --module tasks.$(RUN_ARGS)
