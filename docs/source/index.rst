.. Bigmetadata documentation master file, created by
   sphinx-quickstart on Tue Aug  2 16:07:23 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Bigmetadata ETL Docs
====================

All data for CARTO's `Data Observatory <https://carto.com/data-observatory/>`_
is obtained through tasks built subclassing Bigmetadata ETL classes.

The classes themselves are derived from `Luigi
<http://luigi.readthedocs.org/>`_ tasks.

By performing the ETL using these classes, we gain a few guarantees:

* Reproduceability, and avoidance of duplicate work
* Generation of high-quality metadata consumable by the Observatory API
* Scalability across multiple processes

Contents:

.. toctree::
    :maxdepth: 2

    quickstart
    example
    development
    convenience
    metadata
    validation
    testing
    deployment


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

