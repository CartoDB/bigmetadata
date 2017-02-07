Testing your data
=================

.. contents::
   :local:
   :depth: 2

ETL unit tests
--------------

Unit tests ensure that there are no errors in the underlying utility classes
that could cause errors in code you build on top of them.

The tests also provide limited coverage for simple :ref:`tasks.util.ColumnTask`
classes that don't need parameters set.

Tests are run with:

    make etl-unittest

API unit tests
--------------

API unit tests make sure the observatory-extension, which reads data and
metadata from the ETL, are working right.

``TODO``

Integration tests
-----------------

Integration tests ensure that the data from the ETL that is set for deployment
is is able to return a measure for every piece of metadata.

``TODO``

Diagnosing common issues in integration tests
---------------------------------------------

Cannot find point to test
*************************

``TODO``

