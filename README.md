[![Build Status](https://api.travis-ci.org/CartoDB/bigmetadata.svg?branch=master)](https://travis-ci.org/CartoDB/bigmetadata/branches#) [![Documentation Status](https://readthedocs.org/projects/bigmetadata/badge/?version=latest)](http://bigmetadata.readthedocs.io/en/latest/?badge=latest)

## Bigmetadata

### What it is

Bigmetadata comprises four parts:

    ETL | Observatory | Metadata | Catalog

Everything is dockerized and can be run on a standalone EC2 instance provided
Docker is available.

* __ETL__: Luigi tasks that extract data from anywhere on the web, transform the
  data in modular and observable steps, generate metadata, and upload
  observatory datasets in the process.  While the ETL pushes transformed
  datasets to CartoDB, it actually is backed by its own separate
  Postgres/PostGIS database.

* __Observatory__: transformed and standardized datasets living on a [CartoDB
  instance](https://observatory.cartodb.com).  This is where we can pull our
  actual data from, whether shuffling bytes or using PL/Proxy.  This is also
  where preview visuals using widgets or other Carto JS interfaces can get
  their underlying data from.

* __Metadata__: human and machine-readable descriptions of the data in the
  observatory.  Table schema can be found in
  [tasks/meta.py](https://github.com/CartoDB/bigmetadata/blob/master/tasks/meta.py#L76).
  There are six related tables, `obs_table`, `obs_column_table`, `obs_column`,
  `obs_column_tag`, `obs_tag`, and `obs_column_to_column`.  An overarching
  denormalized view can be found in `obs_meta`.

* __Catalog__: a [static HTML guide](https://cartodb.github.io/bigmetadata) to
  data in the observatory generated from the metadata.  Docs are generated
  using [Sphinx](http://sphinx-doc.org/) and hosted on GitHub pages.

### Installation

See [QUICKSTART](docs/source/quickstart.rst).

### Tasks

Most of the common tasks have already been wrapped up in the `Makefile`:

* `make catalog`: Regenerate the catalog
* `make deploy-html-catalog`: Deploy the catalog to Github Pages
* `make sh`: Drop into the bigmetadata container to run shell scripts
* `make python`: Drop into an interactive Python shell in the bigmetadata
  container
* `make psql`: Drop into an interactive psql session in the bigmetadata
  container's database
* `make rebuild-all`: Task to clean and rebuild all the `*-all` tasks. You can disable the heavy tasks, which by now is `us-all` adding `RUN_HEAVY_TASKS=false`

The ETL tasks have also already been wrapped up in the Makefile:

* `make [docker-]au-all`: ETL the entirety of the Australian data
* `make [docker-]br-all`: ETL the entirety of the Brazilian data
* `make [docker-]ca-all`: ETL the entirety of the Canadian data
* `make [docker-]es-all`: ETL the entirety of the Spanish data
* `make [docker-]eu-all`: ETL the entirety of the Eurostat data
* `make [docker-]fr-all`: ETL the entirety of the French data
* `make [docker-]mx-all`: ETL the entirety of the Mexican data
* `make [docker-]uk-all`: ETL the entirety of the United Kingdom data
* `make [docker-]us-all`: ETL the entirety of the United States data

Any other task can be run using `docker-compose`:

    docker-compose run bigmetadata luigi --module tasks.path.to.task \
      TaskName --param1 val1 --param2 val2

Or, more conveniently, `make -- run` (which will use the local scheduler):

    make -- run path.to.task.TaskName --param1 val1 --param2 val2

For example, to run QCEW numbers for one quarter:

    make -- run us.bls.QCEW --year 2014 --qtr 4

Or using Docker:

    make -- docker-run us.bls.QCEW --year 2014 --qtr 4

If you want to use the local scheduler, you can add `SCHEDULER=--local-scheduler` to the `make` task

### Execution watcher

We've added a new script to watch for containers execution and notify when it ends. Is located in `scripts/watch_containers.py`

```
usage: python3 watch_containers.py [-h] [--since SINCE]
                                   [--pooling-time POOLING_TIME]
                                   [--notification-channel {stdout,slack,logfile}]
                                   name
```

Let me explain the options:

* **name**: Name, complete or a part of it, of the container to watch
* **since**: [optional] Date in the `YYYY-mm-dd HH:MM:SS` UTC format since the script starts looking for docker containers to watch. Default value is [utcnow()](https://docs.python.org/2/library/datetime.html#datetime.datetime.utcnow) value
* **pooling-time**: [optional] How many seconds we check for containers completion. Default value is 60 seconds
* **notification-channel**: [optional] Where to send the notifications: stdout or slack. Default value is `stdout`

For the slack notification you need to have an environment variable called `SLACK_WEBHOOK` that contains the URL for the [incoming webhook](https://api.slack.com/incoming-webhooks) you've created to send the messages.

### Setting up local directories

`docker-compose.yml` allows configuring two environment variables:
- `DO_LOCAL_POSTGRESQL_LIB_DIR` (default: `./postgres/data`).
- `DO_LOCAL_POSTGRESQL_TMP_DIR` (default: `./tmp`).

That's useful if you have an external HD for storing DO.

### Naming conventions

#### Tables

In bigmetadata's postgres, tables live in the `observatory` schema, with an
auto-generated hash for the tablename prefixed by `obs` (EG `obs_<hash>`).
In `obs_table`, the table ID refers back to the task which created that table
as well as the parameters called.

#### Columns

Column IDs in metadata are fully-qualified, like tables, such as
`us.bls.avg_wkly_wage_trade_transportation_and_utilities`.  In tables
themselves, column names are the `colname` from metadata,
so the above would be `avg_wkly_wage_trade_transportation_and_utilities` in
an actual table.

### Metadata

#### Why favor descriptions of columns over tables in metadata?

Our users need third party data in their maps to make it possible for them to
better interpret their own data.

In order to make this as simple as possible, we need to rethink the prevailing
model of finding external data by where it's sourced from.  Instead, we should
think of finding external data by need.

For example, instead of requiring our user to think "I need race or demographic
data alongside my data, I'm in the US, so I should look at the census", we
should enable our user to look up race or demographic data -- and figure out
which columns they need from the census without particularly worrying about
delving into the source.
