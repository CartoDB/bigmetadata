Testing your data
=================

.. contents::
   :local:
   :depth: 2

ETL unit tests
--------------

Unit tests ensure that there are no errors in the underlying utility classes
that could cause errors in code you build on top of them.

Tests are run with:

.. code:: shell

    make etl-unittest

..
Metadata integration tests
--------------------------

Integration tests make sure that the metadata being generated as part of your
ETL will actually be queryable by the API.  For example, if you have an ETL
that ingests data but does not 
..

API unit tests
--------------

API unit tests make sure the observatory-extension, which reads data and
metadata from the ETL, are working right.

In order for this to function, you'll need to clone a copy of
``observatory-extension`` into the root of the ``bigmetadata`` repo.

.. code:: shell

    git clone git@github.com:cartodb/observatory-extension
    make extension-unittest

Integration tests
-----------------

Integration tests ensure that the data from the ETL that is set for deployment
is is able to return a measure for every piece of metadata.

As above, you'll need a copy of ``observatory-extension`` locally for this test
to work.

.. code:: shell

    git clone git@github.com:cartodb/observatory-extension
    make extension-autotest

Diagnosing common issues in integration tests
---------------------------------------------

Cannot find point to test
*************************

``TODO``

