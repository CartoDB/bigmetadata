Development
===========

Writing ETL tasks is pretty repetitive.  In :py:mod:`tasks.util` are a
number of functions and classes that are meant to make life easier through
reusability.

.. contents::
   :local:
   :depth: 2

Utility Functions
-----------------

These functions are very frequently used within the methods of a new ETL task.

.. autofunction:: tasks.meta.current_session

.. autofunction:: tasks.util.shell

.. autofunction:: tasks.util.underscore_slugify

.. autofunction:: tasks.util.classpath

Abstract classes
----------------

These are the building blocks of the ETL, and should almost always be
subclassed from when writing a new process.

.. autoclass:: tasks.util.TempTableTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.ColumnsTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.TagsTask
   :members:
   :show-inheritance:

Batteries included
------------------

Data comes in many flavors, but sometimes it comes in the same flavor over and
over again.  These tasks are meant to take care of the most repetitive aspects.

.. autoclass:: tasks.util.DownloadUnzipTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.Shp2TempTableTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.CSV2TempTableTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.Carto2TempTableTask
   :members:
   :show-inheritance:
