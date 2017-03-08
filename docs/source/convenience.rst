Convenience tasks
=================

There are a number of tasks and functions useful for basic, repetitive
operations like interacting with or uploading tables to CARTO.

.. contents::
   :local:
   :depth: 2

Makefile
--------

The Makefile makes it easier to run tasks.

.. _run-any-task:

Run any task
************

Any task can be run with:

.. code:: shell

   make -- run path.to.module ClassName --param-name-1 value1 --param-name-2 value2

For example:

.. code:: shell

   make -- run us.bls QCEW --year 2014 --qtr 4

Other tasks
***********

* ``make dump``: Runs :class:`~.carto.DumpS3`

* ``make restore <path/to/dump>``: Restore database from a ``dump``.

* ``make sync-meta``: Runs :class:`~.carto.SyncMetadata`

* ``make sync-data``: Runs :class:`~.carto.SyncAllData`

* ``make sh``: Drop into an interactive shell in the Docker container

* ``make psql``: Drop into an interactive psql session in the database

* ``make kill``: Kill all Docker processes

* ``make docs``: Regenerate all documentation

* ``make catalog``: Regenerate the HTML catalog

Tasks
-----

.. autoclass:: tasks.util.TableToCartoViaImportAPI
   :members:

.. autoclass:: tasks.carto.DumpS3
   :members:

.. autoclass:: tasks.carto.SyncMetadata
   :members:

.. autoclass:: tasks.carto.SyncAllData
   :members:

.. autoclass:: tasks.carto.Import
   :members:

Functions
---------

.. autofunction:: tasks.util.query_cartodb

.. autofunction:: tasks.util.import_api

.. autofunction:: tasks.util.sql_to_cartodb_table

