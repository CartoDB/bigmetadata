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

Provided :ref:`tasks.util.TableTask` and `tasks.util.ColumnTask` classes were
executed, it's wise to jump into the database and check to make sure entries
were made in those tables.

    make psql

    SELECT COUNT(*) FROM observatory.obs_column WHERE id LIKE 'path.to.module.%';

    SELECT COUNT(*) FROM observatory.obs_table WHERE id LIKE 'path.to.module.%';

    SELECT COUNT(*) FROM observatory.obs_column_table
    WHERE column_id LIKE 'path.to.module%'
      AND table_id  LIKE 'path.to.module%';

Delete old data to start from scratch to make sure everything works
*******************************************************************

When using the proper utility classes, your data on disk, for example from
downloads that are part of the ETL, will be saved to a file or folder
`tmp/module.name/ClassName_Args`.

In order to make sure the ETL is reproduceable, it's wise to delete this
folder or move it to another location after development, and re-run to make
sure that the whole process can still run from start to finish.

Making sure metadata works right
--------------------------------

Checking the metadata works right is one of the more challenging components of
QA'ing new ETL code.


