#!/bin/bash -e

docker run \
  -v $PWD:/bigmetadata \
  --net=host --env-file=.env.sample \
             -e PGHOST=localhost -e PYTHONPATH=/bigmetadata \
             -e PGDATABASE=test -e PGUSER=postgres \
             -e LC_ALL=C.UTF-8 -e LANG=C.UTF-8 \
             -e LUIGI_CONFIG_PATH=/bigmetadata/conf/luigi_client.cfg \
             -e TRAVIS=$TRAVIS \
   recessionporn/bigmetadata /bin/bash -c "$1"
