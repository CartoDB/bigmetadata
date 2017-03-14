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

.. _abstract-classes

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

Running and Re-Running Pieces of the ETL
-------------------------

When doing local development, it's advisable to run small pieces of the ETL
locally to make sure everything works correctly.  You can use the ``make --
run`` helper, documented in :ref:`run-any-task`. There are several methods for
re-running pieces of the ETL depending on the task and are described below:

Using ``--force`` during development
************************************

When developing with :ref:`abstract-classes` that offer a ``force`` parameter,
you can use it to re-run a task that has already been run, ignoring and
overwriting all output it has already created.  For example, if you have
a :ref:`tasks.util.TempTableTask` that you've modified in the course of
development and need to re-run:

.. code:: python

    from tasks.util import TempTableTask
    from tasks.meta import current_session

    class MyTempTable(TempTableTask):

        def run(self):
            session = current_session()
            session.execute('''
               CREATE TABLE {} AS SELECT 'foo' AS mycol;
            ''')

Running ``make -- run path.to.module MyTempTable`` will only work once, even
after making changes to the ``run`` method.

However, running ``make -- run path.to.module MyTempTable --force`` will force
the task to be run again, dropping and re-creating the output table.

Deleting byproducts to force a re-run of parts of ETL
*****************************************************

In some cases, you may have a :ref:`luigi.Task` you want to re-run, but does
not have a ``force`` parameter.  In such cases, you should look at its
``output`` method and delete whatever files or database tables it created.

Utility classes will put their file byproducts in the ``tmp`` folder, inside
a folder named after the module name.  They will put database byproducts into
a schema that is named after the module name, too.

Update the ETL & metadata through ``version``
*********************************************

When you make changes and improvements, you can increment the ``version``
method of :ref:`tasks.util.TableTask`, :ref:`tasks.util.ColumnsTask` and
:ref:`tasks.util.TagsTask` to force the task to run again.
