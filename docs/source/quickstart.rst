Quickstart
==========

Requirements
------------

You'll need:

* `git <https://git-scm.com/>`_
* `docker <https://www.docker.com>`_
* `docker-compose <https://docs.docker.com/compose/>`_

You should also install `make <https://www.gnu.org/software/make/>`_ to
get access to convenience commands, if you don't have it already.

You'll want at least 2GB of memory available on the host machine.

You'll want at least 30GB of disk space available on the host machine to work
comfortably with data and get everything running.  If you want to install
an existing database dump, you will need more like 120GB of space.  If you want
to install, say, the entire American Community Survey, you will want more like
1TB of space.

Clone & configure
-----------------

Once your prerequisites are set up, clone the repo:

.. code:: shell

  git clone https://github.com/cartodb/bigmetadata.git
  cd bigmetadata

You'll then need to configure ``CARTODB_API_KEY`` and ``CARTODB_URL`` in the
``.env`` file in order to upload to Carto.  Replace the variable values in
brackets and execute each line below.

.. code:: shell

  echo CARTODB_API_KEY=<YOUR_API_KEY> > .env
  echo CARTODB_URL=<YOUR_CARTO_URL> >> .env

The ``CARTODB_URL`` should be the one you use for the SQL API, generally of the
format ``<USERNAME>.carto.com``.

If you're on Linux instead of Mac, you may want to give your existing user
docker (which is equivalent to root) privileges:

.. code:: shell

  sudo gpasswd -a $(whoami) docker

Then log out, and log in.

Build
-----

.. caution::

   On Docker for Mac, it's not possible to link a local volume into Postgres.
   You'll need to comment out the line
   ``"./postgres/data:/var/lib/postgresql"`` in ``docker-compose.yml`` in
   order to get the ``postgres`` container running.  However, this also means
   you'll lose data if you rebuild the ``postgres`` container.

Most of the requirements are available as images, but you will need
to build the postgres and bigmetadata containers before getting started.

.. code:: shell

  docker-compose build

If you're running into errors related to missing packages in the build, try
building without the cache:

.. code:: shell

  docker-compose build --no-cache

Run
---

Then get all your containers running in the background:

.. code:: shell

  docker-compose up -d

Once everything is up and running, you should be able to run a task.

.. code:: shell

  make -- run es.ine FiveYearPopulation

That will run :class:`~.es.ine.FiveYearPopulation`.  This includes downloading
all the source data files if they don't already exist locally, and generating
all the metadata necessary to make this dataset work with
`observatory-extension <https://github.com/CartoDB/observatory-extension>`_
functions.

You can take a look at the data:

.. code:: shell

  make psql

  gis=# select count(*) from observatory.obs_column;
   count
  -------
     169
  (1 row)

  gis=# select id, name, type, aggregate from observatory.obs_column where name ilike 'population%';

               id          |            name            |  type   | aggregate
  -------------------------+----------------------------+---------+-----------
   es.ine.pop_0_4          | Population age 0 to 4      | Numeric | sum
   es.ine.pop_5_9          | Population age 5 to 9      | Numeric | sum
   es.ine.pop_10_14        | Population age 10 to 14    | Numeric | sum
   es.ine.pop_15_19        | Population age 15 to 19    | Numeric | sum
   es.ine.pop_20_24        | Population age 20 to 24    | Numeric | sum
   es.ine.pop_25_29        | Population age 25 to 29    | Numeric | sum
   es.ine.pop_30_34        | Population age 30 to 34    | Numeric | sum
   es.ine.pop_35_39        | Population age 35 to 39    | Numeric | sum
   es.ine.pop_40_44        | Population age 40 to 44    | Numeric | sum
   es.ine.pop_45_49        | Population age 45 to 49    | Numeric | sum
   es.ine.pop_50_54        | Population age 50 to 54    | Numeric | sum
   es.ine.pop_55_59        | Population age 55 to 59    | Numeric | sum
   es.ine.pop_60_64        | Population age 60 to 64    | Numeric | sum
   es.ine.pop_65_69        | Population age 65 to 69    | Numeric | sum
   es.ine.pop_70_74        | Population age 70 to 74    | Numeric | sum
   es.ine.pop_75_79        | Population age 75 to 79    | Numeric | sum
   es.ine.pop_80_84        | Population age 80 to 84    | Numeric | sum
   es.ine.pop_85_89        | Population age 85 to 89    | Numeric | sum
   es.ine.pop_90_94        | Population age 90 to 94    | Numeric | sum
   es.ine.pop_95_99        | Population age 95 to 99    | Numeric | sum
   es.ine.pop_100_more     | Population age 100 or more | Numeric | sum
  (21 rows)

  gis=# select * from observatory.obs_column_to_column where source_id in (select id from observatory.obs_column where name ilike 'population%');

            source_id      |  target_id  |   reltype
  -------------------------+-------------+-------------
   es.ine.pop_0_4          | es.ine.t1_1 | denominator
   es.ine.pop_5_9          | es.ine.t1_1 | denominator
   es.ine.pop_10_14        | es.ine.t1_1 | denominator
   es.ine.pop_15_19        | es.ine.t1_1 | denominator
   es.ine.pop_20_24        | es.ine.t1_1 | denominator
   es.ine.pop_25_29        | es.ine.t1_1 | denominator
   es.ine.pop_30_34        | es.ine.t1_1 | denominator
   es.ine.pop_35_39        | es.ine.t1_1 | denominator
   es.ine.pop_40_44        | es.ine.t1_1 | denominator
   es.ine.pop_45_49        | es.ine.t1_1 | denominator
   es.ine.pop_50_54        | es.ine.t1_1 | denominator
   es.ine.pop_55_59        | es.ine.t1_1 | denominator
   es.ine.pop_60_64        | es.ine.t1_1 | denominator
   es.ine.pop_65_69        | es.ine.t1_1 | denominator
   es.ine.pop_70_74        | es.ine.t1_1 | denominator
   es.ine.pop_75_79        | es.ine.t1_1 | denominator
   es.ine.pop_80_84        | es.ine.t1_1 | denominator
   es.ine.pop_85_89        | es.ine.t1_1 | denominator
   es.ine.pop_90_94        | es.ine.t1_1 | denominator
   es.ine.pop_95_99        | es.ine.t1_1 | denominator
   es.ine.pop_100_more     | es.ine.t1_1 | denominator
  (21 rows)

  gis=# select id, name, type, aggregate from observatory.obs_column where id = 'es.ine.t1_1';
       id      |       name       |  type   | aggregate
  -------------+------------------+---------+-----------
   es.ine.t1_1 | Total population | Numeric | sum
  (1 row)
