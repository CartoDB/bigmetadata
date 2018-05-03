#!/bin/bash

mkdir /opt/bigmetadata
mkdir /opt/bigmetadata/etc
cp /bigmetadata/conf/luigi_daemon.cfg $LUIGI_CONFIG_PATH
cp /bigmetadata/conf/logging_daemon.cfg $LOGGING_CONFIG_PATH
sed -i 's?@postgres?@'$PGHOST'?' $LUIGI_CONFIG_PATH
sed -i 's?/bigmetadata/conf/logging_daemon.cfg?/opt/bigmetadata/etc/logging_daemon.cfg?' $LUIGI_CONFIG_PATH
chmod +x /usr/local/bin/wait-for-it.sh
wait-for-it.sh --strict $PGHOST:$PGPORT
exec "$@"
