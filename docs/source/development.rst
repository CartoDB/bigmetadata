Development
=============================================

Writing ETL tasks is pretty repetitive.  In `tasks/util.py` are a number of
functions and classes that are meant to make life easier through reusability:

foobar

.. toctree::

    :maxdepth: 2

    Functions
    Classes

Utility Functions
-----------------

.. autofunction:: tasks.util.shell

.. autofunction:: tasks.util.query_cartodb

.. autofunction:: tasks.util.import_api

.. autofunction:: tasks.util.underscore_slugify

.. autofunction:: tasks.util.classpath

.. autofunction:: tasks.util.sql_to_cartodb_table

Utility Classes
---------------

.. autoclass:: tasks.util.TempTableTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.ColumnsTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.TagsTask
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.PostgresTarget
   :members:
   :show-inheritance:

.. autoclass:: tasks.util.CartoDBTarget
   :members:
   :show-inheritance:
