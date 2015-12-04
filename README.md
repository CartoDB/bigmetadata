## Big Metadata

Work on creating a column-based metadata store.

### Running it

Easiest way to get started is with Docker.  Make sure you have Docker and
docker-compose up and running on your system.

    docker-compose run bigmetadata /bigmetadata/run.sh

This will run metadata generation.  By default this will use your local
postgres -- if you want to use a different postgres, specify it via shell
variables in a `.env` file in the the root bigmetadata directory:

    ### .env

    export PGPASSWORD=<mypassword>
    export PGHOST=<myhost>
    export PGUSER=<myuser>
    export PGPORT=<myport>
    export PGOPTIONS="-c default_tablespace=<mytablespace>"

