#!/usr/bin/env bash
set -e

chmod 755 /var/lib/postgresql
mkdir -p "$PGDATA"
chown -R postgres "$PGDATA"
chmod 700 "$PGDATA"
mkdir -p /var/run/postgresql
chown -R postgres /var/run/postgresql
chmod 775 /var/run/postgresql

# look specifically for PG_VERSION, as it is expected in the DB dir
if [ ! -s "$PGDATA/PG_VERSION" ]; then

    su - postgres -c "/usr/lib/postgresql/10/bin/initdb -E UTF8 --locale=en_US.UTF-8 -D $PGDATA"

    # internal start of server in order to allow set-up using psql-client
    # does not listen on external TCP/IP and waits until start finishes
    su - postgres -c "PGUSER=${PGUSER:-postgres} /usr/lib/postgresql/10/bin/pg_ctl -D $PGDATA -w start"

    if [ "$POSTGRES_DB" != 'postgres' ]; then
        echo
        echo "Creating database $POSTGRES_DB..."
        echo
        su - postgres -c "/usr/lib/postgresql/10/bin/createdb -U postgres -E UTF-8 $POSTGRES_DB"
        echo
        echo "Database $POSTGRES_DB created!"
    fi

    if [ "$POSTGRES_USER" != 'postgres' ]; then
        echo
        echo "Creating user $POSTGRES_USER..."
        echo
        su - postgres -c "/usr/lib/postgresql/10/bin/createuser -U postgres -s $POSTGRES_USER"
        echo
        echo "User $POSTGRES_USER created!"
    fi

    echo
    echo 'Creating extension postgis'
    echo
    su - postgres -c  "psql -U ${POSTGRES_USER:-postgres} -d $POSTGRES_DB -c 'CREATE EXTENSION postgis'"
    echo 'Extension postgis created!'

    echo
    echo 'Creating extension postgis_topology'
    echo
    su - postgres -c  "psql -U ${POSTGRES_USER:-postgres} -d $POSTGRES_DB -c 'CREATE EXTENSION postgis_topology'"
    echo 'Extension postgis_topology created!'

    echo
    echo 'Creating extension plpythonu'
    echo
    su - postgres -c  "psql -U ${POSTGRES_USER:-postgres} -d $POSTGRES_DB -c 'CREATE LANGUAGE plpythonu'"
    echo 'Extension plpythonu created!'

    echo
    echo 'Creating extension tablefunc'
    echo
    su - postgres -c  "psql -U ${POSTGRES_USER:-postgres} -d $POSTGRES_DB -c 'CREATE EXTENSION tablefunc;'"
    echo 'Extension tablefunc created!'

    echo
    echo 'PostgreSQL init process complete; ready for start up.'
    echo

    su - postgres -c "PGUSER=${POSTGRES_USER:-postgres} /usr/lib/postgresql/10/bin/pg_ctl -D $PGDATA -m fast -w stop"

fi

exec su - postgres -c "/usr/lib/postgresql/10/bin/postgres -D $PGDATA --config-file=/etc/postgresql/10/main/postgresql.conf"
