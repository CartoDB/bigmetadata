Validating your code
====================

.. contents::
   :local:
   :depth: 2

Best practices
--------------

Writing ETL code is meant to be open-ended, but there are some standards that
should be followed to help keep the code clean and clear.

Proper use of utility classes
*****************************

There are extensive :ref:`abstract-classes` available for development.  These
can do things like download and unzip a file to disk
(:ref:`tasks.util.DownloadUnzipTask`) and import a CSV on disk to a temporary
table (:ref:`tasks.util.CSV2TempTableTask`).  These classes should be used when
possible to minimize specialized ETL code.  In particular, these tasks save
output to well-known locations so as to avoid redundantly running the same
tasks.

Clearly documented command that runs a WrapperTask to create everything
***********************************************************************

Oftentimes an ETL will have to loop over a parameter to get all the data -- for
example, if a dataset is available online year-by-year, it may make sense to
write a single task that downloads one year's file, with an parameter specifying
which year.

:ref:`luigi.WrapperTask` is a way to make sure such tasks are executed for every
relevant parameter programmatically.  A powerful example of this can be found
with :ref:`tasks.us.AllZillow`, which executes a :ref:`tasks.us.zillow.Zillow`
task once for each geography level, year, and month in that year.

A generic example of using a :ref:`luigi.WrapperTask`:

.. code:: python

    from luigi import WrapperTask, Task, Parameter

    class MyTask(Task):
        '''
        This task needs to be run for each possible `geog`
        '''

        geog = Parameter()

        def run(self):
            pass

        def output(self):
            pass


    class MyWrapperTask(WrapperTask):
        '''
        Execute `MyTask` once for each possible `geog`.
        '''

        def requires(self):
            for geog in ('state', 'county', 'city'):
                yield MyTask(geog=geog)

Use parameters only when necessary
**********************************

Tasks are unique to their parameters.  In other words, if a task is run once
with a certain set of parameters, it will not be run again unless the output it
generated is deleted.

Therefore it's very important to not have parameters available in a Task's
definition that do not affect its result.  If you have such extraneous
parameters, it would be possible to run a task redundantly.

An example of this:

.. code:: python

    from tasks.util import DownloadUnzipTask

    class MyBadTask(DownloadUnzipTask):

        goodparam = Parameter()
        badparam = Parameter()

        def url(self):
            return 'http://somesite/with/data/{}'.format(self.goodparam)

:ref:`tasks.util.DownloadUnzipTask` will generate the location for a unique
output file automatically based off of all its params, but ``badparam`` above
doesn't actually affect the file being downloaded.  That means if we change
``badparam`` we'll download the same file twice.

Use default parameter values sparingly
**************************************

The above bad practice is easily paired with setting default values for
parameters.  For example:

.. code:: python

    from tasks.util import DownloadUnzipTask

    class MyBadTask(DownloadUnzipTask):
        '''
        My URL doesn't depend on `badparam`!
        '''

        goodparam = Parameter()
        badparam = Parameter(default='foo')

        def url(self):
            return 'http://somesite/with/data/{}'.format(self.goodparam)

Now it's easy to simply forget that ``badparam`` even exists!  But it still
affects the output filename, making it noisy and less clear which parameters
actually matter.

Keep fewer than 1000 columns per table
**************************************

Postgres has a hard limit on the number of columns.  If you create
a :ref:`tasks.util.TableTask` whose ``columns`` method returns
a :ref:`OrderedDict` with much more than 1000 columns, the task will fail.

In such cases, you'll want to split your :ref:`tasks.util.TableTask` into
several pieces, likely pulling columns from the same
:ref:`tasks.util.ColumnsTask`.  There is no limit on the number of columns in
a :ref:`tasks.util.ColumnsTask`.

Each geometry column should have a unique ``geom_ref`` column with it
*********************************************************************

When setting up a :ref:`tasks.util.ColumnsTask` for Geometries, make sure that
you store a meaningful and unique ``geom_ref`` from the same table.

* It is meaningful if it can be found as a way to refer to that geometry in
  data sources elsewhere -- for example, `FIPS codes
  <https://en.wikipedia.org/wiki/FIPS_county_code>`_ are meaningful references
  to county geometries in the USA.  However, the automatically generated serial
  ``ogc_fid`` column from a Shapefile is not meaningful.
* It is unique if that ``geom_ref`` column has an ID that is not duplicated by
  any other columns.

For example:

.. code:: python

    from tasks.util import ColumnsTask
    from tasks.meta import OBSColumn, GEOM_REF
    from luigi import Parameter

    class MyGeoColumnsTask(ColumnsTask):

        resolution = Parameter()

        def columns(self):

            geom = OBSColumn(
              id=self.resolution,
              type='Geometry')

            geomref = OBSColumn(
              id=self.resolution + '_id',  # Make sure we have "+ '_id'"!
              type='Text',
              targets={geom: GEOM_REF})

            return OrderedDict([
              ('geom', geom),
              ('geomref', geomref)
            ])

No matter what ``resolution`` this Task is passed, it will generate a unique ID
for both the ``geom`` and the ``geomref``.  If the ``+ '+id'`` concatenation
were missing, it would mean that the metadata model would not properly link
geomrefs to the geometries they refer to.

Specify section, subsection, source tags and license tags for all columns
****************************************************

When defining your :ref:`tasks.meta.OBSColumn` objects in
a :ref:`tasks.util.ColumnsTask` class, make sure each column is assigned
a :ref:`tasks.util.OBSTag` of ``type``, ``section``, ``subsection``, ``source``,
and ``license``.  Use shared tags from :ref:`tasks.tags` when possible, in
particular for ``section`` and ``subsection``.

Specify unit tags for all measure columns
*****************************************

When defining a :ref:`tasks.meta.OBSColumn` that will hold a measurement, make
sure to define a ``unit`` using a tag.  This could be something like
``people``, ``money``, etc.  There are standard units accessible in
:ref:`tasks.tags`.

Making sure ETL code works right
--------------------------------

After having written an ETL, you'll want to double check all of the following
to make sure the code is usable.

Results and byproducts are being generated
******************************************

When you use :ref:`run-any-task` to run individual components:

* Were any exceptions thrown?  On what task were they thrown?  With which
  arguments?
* Are appropriate files being generated in the ``tmp`` folder?
* Are tables being created in the relevant ``tmp`` schema?
* Are tables and columns being added to the ``observatory.obs_table`` and
  ``observatory.obs_column`` metadata tables?

Provided :ref:`tasks.util.TableTask` and :ref:`tasks.util.ColumnTask` classes were
executed, it's wise to jump into the database and check to make sure entries
were made in those tables.

.. code:: shell

    make psql

.. code:: sql

    SELECT COUNT(*) FROM observatory.obs_column WHERE id LIKE 'path.to.module.%';

    SELECT COUNT(*) FROM observatory.obs_table WHERE id LIKE 'path.to.module.%';

    SELECT COUNT(*) FROM observatory.obs_column_table
    WHERE column_id LIKE 'path.to.module%'
      AND table_id  LIKE 'path.to.module%';

Delete old data to start from scratch to make sure everything works
*******************************************************************

When using the proper utility classes, your data on disk, for example from
downloads that are part of the ETL, will be saved to a file or folder
``tmp/module.name/ClassName_Args``.

In order to make sure the ETL is reproduceable, it's wise to delete this
folder or move it to another location after development, and re-run to make
sure that the whole process can still run from start to finish.

Making sure metadata works right
--------------------------------

Checking the metadata works right is one of the more challenging components of
QA'ing new ETL code.

Regenerate the ``obs_meta`` table
*********************************

The ``obs_meta`` table is a denormalized view of the underlying :ref:`metadata` 
objects that you've created when running tasks.

You can force the regeneration of this table using
:ref:`tasks.carto.OBSMetaToLocal`

.. code:: shell

    make -- run carto OBSMetaToLocal

Once the table is generated, you can take a look at it in SQL:

.. code:: shell

    make psql

If the metadata is working correctly, you should have more entries in
``obs_meta`` than before.  If you were starting from nothing, there should be
more than 0 rows in the table.

.. code:: sql

    SELECT COUNT(*) FROM observatory.obs_meta;

If you already had data, you can filter ``obs_meta`` to look for new rows with
a schema corresponding to what you added.  For example, if you added metadata
columns and tables in ``tasks/mx/inegi``, you should look for columns with that
schema:

.. code:: sql

    SELECT COUNT(*) FROM observatory.obs_meta WHERE numer_id LIKE 'mx.inegi.%';

If nothing is appearing in ``obs_meta``, chances are you are missing some
metadata:

Have you defined and executed a proper :ref:`tasks.util.TableTask`?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can check to see if these links exist by checking ``obs_column_table``:

.. code:: shell

    make psql

.. code:: sql

    SELECT COUNT(*) FROM observatory.obs_column_table
    WHERE column_id LIKE 'my.schema.%'
      AND table_id LIKE 'my.schema.%';

If they don't exist, make sure that your Python code roughly corresponds to:

.. code:: python

    from tasks.util import ColumnsTask, TableTask

    class MyColumnsTask(ColumnsTask):

        def columns(self):
            # Return OrderdDict of columns here

    class MyTableTask(TableTask):

        def timespan(self):
            # Return timespan here

        def requires(self):
            return {
                'columns': MyColumnsTask()
             }

        def columns(self):
            return self.input()['columns']

        def populate(self):
            # Populate the output table here

Unless the :ref:`TableTask` returns some of the columns from :ref:`ColumnsTask`
in its own ``columns`` method, the links will not be initialized properly.

Finally, double check that you actually ran the :ref:`TableTask` using ``make
-- run my.schema MyTableTask``.

Are you defining ``geom_ref`` relationships properly?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In cases where a :ref:`TableTask` does not have its own geometries, at least
one of the columns returned from its ``columns`` method needs to be in
a ``geom_ref`` relationship.  Here's an example:

.. code:: python

    from collections import OrderedDict

    from tasks.util import ColumnsTask, TableTask
    from tasks.meta import OBSColumn, GEOM_REF

    class MyGeoColumnsTask(ColumnsTask):
        def columns(self):

            geom = OBSColumn(
              type='Geometry')

            geomref = OBSColumn(
              type='Text',
              targets={geom: GEOM_REF})

            return OrderedDict([
              ('geom', geom),
              ('geomref', geomref)
            ])

    class MyColumnsTask(ColumnsTask):

        def columns(self):
            # Return OrderdDict of columns here

    class MyTableTask(TableTask):

        def timespan(self):
            # Return timespan here

        def requires(self):
            return {
                'geom_columns': MyGeoColumnsTask(),
                'data_columns': MyColumnsTask()
             }

        def columns(self):
            cols = OrderedDict()
            cols['geomref'] = self.input()['geom_columns']['geomref']
            cols.update(self.input()['data_columns'])
            return cols

        def populate(self):
            # Populate the output table here

The above code would ensure that all columns existing inside ``MyTableTask``
would be appropriately linked to any geometries that connect to ``geomref``.

Do you have both the data and geometries in your table?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can check by running:

.. code:: sql

    SELECT * FROM observatory.obs_table
    WHERE id LIKE 'my.schema.%';

If there is only one table and it has a null "the_geom" boundary,
then you are missing a geometry table. For example:

.. code:: sql

   SELECT * from observatory.obs_table
   WHERE id LIKE 'es.ine.five_year_population%';

.. code:: shell

                      id                   |                  tablename                   | timespan | the_geom | description | version 
   ----------------------------------------+----------------------------------------------+----------+----------+-------------+---------
    es.ine.five_year_population_99914b932b | obs_24b656e9e23d1dac2c8ab5786a388f9bf0f4e5ae | 2015     |          |             |       5
   (1 row)

Notice that the_geom is empty. You will need to write a second :ref:`TableTask` with the 
following structure:

.. code:: python

   class Geometry(TableTask):

        def timespan(self):
            # Return timespan here

        def requires(self):
           return {
               'meta': MyGeoColumnsTask(),
               'data': RawGeometry()
           }

       def columns(self):
           return self.input()['meta']

        def populate(self):
            # Populate the output table here

Regenerate and look at the Catalog
--------------

Once :ref:`tasks.carto.OBSMetaToLocal` has been run, you can generate the
catalog.

.. code:: shell

     make catalog

You can view the generated Catalog in a browser window by going to the IP and
port address for the nginx process. The current processes are shown with
``docker-compose ps`` or ``make ps``.

1. Are there any nasty typos or missing data?

   * Variable names should be unique, human-readable, and concise. If the
     variable needs more in-depth definition, this should go in the
     "description" of the variable.

2. Does the nesting look right?  Are there columns not nested?

   * Variables that are denominators should also have subcolumns of direct
     nested variables.

   * There may be repetitive nesting if a variable is nested under two
     denominators, which is fine.

3. Are sources and licenses populated for all measures?

   * A source and license :ref:`tasks.util.OBSTag` must be written for new
     sources and licenses

4. Is a table with a boundary/timespan matrix appearing beneath each measure?

   * If not, hardcode the sample latitude and longitude in :ref:`tasks.meta.catalog_lonlat`.


Upload to a test CARTO server
--------------

If you set a ``CARTODB_API_KEY`` and ``CARTODB_URL`` in your ``.env`` file, in
the format:

.. code:: shell

    CARTODB_API_KEY=your_api_key
    CARTODB_URL=https://username.carto.com

You will now be able to upload your data and metadata to CARTO for previewing.

.. code:: shell

    make sync
