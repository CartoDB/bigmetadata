#!/bin/bash

mkdir /opt/bigmetadata
mkdir /opt/bigmetadata/etc
cp /bigmetadata/conf/luigi_client.cfg $LUIGI_CONFIG_PATH
cp /bigmetadata/conf/logging_client.cfg $LOGGING_CONFIG_PATH
sed -i 's?/bigmetadata/conf/logging_client.cfg?/opt/bigmetadata/etc/logging_client.cfg?' $LUIGI_CONFIG_PATH
sed -i 's?tmp/logs/etl_client.log?/bigmetadata/tmp/logs/'"$LOGGING_FILE"'?' $LOGGING_CONFIG_PATH
exec "$@"
