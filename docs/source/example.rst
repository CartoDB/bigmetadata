Example ETL/metadata pipeline
=============================

This is a quick guide to building a full ETL pipeline, along with
associated metadata, for the Data Observatory.

As an example, we will bring in the `Quarterly Census of Employment and
Wages <http://www.bls.gov/cew/>`__ (QCEW), a product of the Bureau of
Labor Statistics. This dataset tracks the number of employees, firms,
and average wages across the full gamut of `North American Industry
Classification System <http://www.census.gov/eos/www/naics/>`__ (NAICS)
industries.

QCEW is, of course, a quarterly release, and counties are the smallest
geography considered.

The process of building a Python module to bring a new dataset into the
Data Observatory can be broadly divided into six steps:

1. Import libraries
2. Download the data
3. Import data into PostgreSQL
4. Preprocess data in PostgreSQL
5. Write metadata
6. Populate output table

We use `Luigi <https://luigi.readthedocs.io/en/stable/>`__ to isolate
each step into a ``Task``. A ``Task`` has well-defined inputs (other
tasks) and outputs (files, tables on disk, etc.) In a nutshell:

-  a task cannot be run if it is complete
-  if all of a ``Task``'s outputs exist, then it is complete
-  in order to run, all of a ``Task``'s requirements must be complete

Each of the steps except (1) corresponds to a ``Task``.

We use a set of utility classes to avoid writing repetitive code.

1. Import libraries
-------------------

.. code:: python

    # Import a test runner
    
    from tests.util import runtask

.. code:: python

    # We'll need these basic utility classes and methods
    
    from tasks.util import (TempTableTask, TableTask, ColumnsTask,
                            DownloadUnzipTask, CSV2TempTableTask,
                            underscore_slugify, shell, classpath)
    from tasks.meta import current_session, DENOMINATOR
    
    # We like OrderedDict because it makes it easy to pass dicts
    # like {column name : column definition, ..} where order still
    # can matter in SQL
    from collections import OrderedDict
    from luigi import IntParameter, Parameter
    import os

.. code:: python

    # These imports are useful for checking the database
    
    from tasks.meta import OBSTable, OBSColumn, OBSTag

.. code:: python

    # We'll also want these tags for metadata
    
    from tasks.tags import SectionTags, SubsectionTags, UnitTags

2. Download the data
--------------------

The first step of most ETLs is going to be downloading the source and
saving it to a temporary folder.

``DownloadUnzipTask`` is a utility class that handles the file naming
and unzipping of the temporary output for you. You just have to write
the code which will do the download to the output file name.

.. code:: python

    class DownloadQCEW(DownloadUnzipTask):
        
        year = IntParameter()
        
        URL = 'http://www.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip'
        
        def download(self):
            shell('wget -O {output}.zip {url}'.format(
               output=self.output().path,
               url=self.URL.format(year=self.year)
            ))

Within the IPython environment, we can create and run the task within a
sandbox.

We have to specify the year, since it's specified as a parameter without
a default.

.. code:: python

    download_task = DownloadQCEW(year=2014)
    runtask(download_task)

Provided the output folder exists, the ``DownloadQCEW`` task for 2014
will not run again.

.. code:: python

    download_task.output().path




.. parsed-literal::

    'tmp/tmp/DownloadQCEW_2014_cfabf27024'



.. code:: python

    download_task.output().exists()




.. parsed-literal::

    True



3. Import data into PostgreSQL
------------------------------

A lot of processing can be done in PostgreSQL quite easily. We have
utility classes to more easily bring both Shapefiles and CSVs into
PostgreSQL.

For ``CSV2TempTableTask``, we only have to define an ``input_csv``
method that will return a path (or iterable of paths) to the CSV(s). The
header row will automatically be checked and used to construct a schema
to bring the data in.

The standard ``requires`` method of Luigi is used here, too. This
requires that the ``DownloadQCEW`` task for the same year must be run
beforehand; the ``output`` from that task is now accessible as the
``input`` of this one.

.. code:: python

    class RawQCEW(CSV2TempTableTask):
        
        year = IntParameter()
        
        def requires(self):
            return DownloadQCEW(year=self.year)
            
        def input_csv(self):
            return os.path.join(self.input().path,'{}.q1-q4.singlefile.csv'.format(self.year))

Run the task. If the table exists and has more than 0 rows, it will not
be run again.

.. code:: python

    
    current_session().rollback()
    raw_task = RawQCEW(year=2014)
    
    runtask(raw_task)

Confirm the task has completed successfully.

.. code:: python

    raw_task.complete()




.. parsed-literal::

    True



Session can be used to execute raw queries against the table.

The output of a ``TempTableTask`` can be queried directly by using its
``table`` method, which is a string with the fully schema-qualified
table name. We are guaranteed names that are unique to the
module/task/parameters without having to come up with any names
manually.

.. code:: python

    raw_task.output().table




.. parsed-literal::

    '"tmp".RawQCEW_2014_cfabf27024'



.. code:: python

    session = current_session()
    resp = session.execute('select count(*) from {}'.format(raw_task.output().table))
    resp.fetchall()




.. parsed-literal::

    [(14276508L,)]



4. Preprocess data in PostgreSQL
--------------------------------

QCEW data has a lot of rows we don't actually need -- these can be
filtered out in SQL easily.

For QCEW, the download files are annual, but contain quarterly time
periods. Output tables should be limited to a single point in time.
We're also only interested in private employment (``own_code = '5'``)
and county level aggregation by total (71), supersector (73), and NAICS
sector (74).

.. code:: python

    class SimpleQCEW(TempTableTask):
        
        year = IntParameter()
        qtr = IntParameter()
        
        def requires(self):
            return RawQCEW(year=self.year)
        
        def run(self):
            session = current_session()
            session.execute("CREATE TABLE {output} AS "
                            "SELECT * FROM {input} "
                            "WHERE agglvl_code IN ('74', '73', '71') "
                            "  AND year = '{year}' "
                            "  AND qtr = '{qtr}' "
                            "  AND own_code = '5' ".format(
                                input=self.input().table,
                                output=self.output().table,
                                year=self.year,
                                qtr=self.qtr,
                           ))

Run the task and confirm it completed. We don't have to run each step as
we write it, as requirements guarantee anything required will be run.

.. code:: python

    simple_task = SimpleQCEW(year=2014, qtr=4)
    runtask(simple_task)
    simple_task.complete()




.. parsed-literal::

    True



.. code:: python

    simple_task.output().table




.. parsed-literal::

    '"tmp".SimpleQCEW_4_2014_79152e4934'



.. code:: python

    resp = session.execute('select count(*) from {}'.format(simple_task.output().table))
    resp.fetchall()




.. parsed-literal::

    [(97167L,)]



5. Write metadata
-----------------

We have to create metadata for the measures we're interested in from
QCEW. Often metadata don't take parameters, but this one is, since we
have to reorganize the table from one row per NAICS code to one column
per NAICS code, which is easiest done programmatically.

The ``ColumnsTask`` provides a structure for generating metadata. The
only required method is ``columns``. What must be returned from that
method is an ``OrderedDict`` whose values are all ``OBSColumn`` and
whose keys are all strings. The keys may be used as human-readable
column names in tables based off this metadata, although that is not
always the case. If the ``id`` of the ``OBSColumn`` is left blank, the
dict's key will be used to generate it (qualified by the module).

Also, conventionally there will be a ``requires`` method that brings in
our standard tags: ``SectionTags``, ``SubsectionTags``, and
``UnitTags``. This is an example of defining several tasks as
prerequisites: the outputs of those tasks will be accessible via
``self.input()[<key>]`` in other methods.

.. code:: python

    from tasks.us.naics import (NAICS_CODES, is_supersector, is_sector,
                                get_parent_code)
    
    class QCEWColumns(ColumnsTask):
        
        naics_code = Parameter()
        
        def requires(self):
            requirements = {
                'sections': SectionTags(),
                'subsections': SubsectionTags(),
                'units': UnitTags(),
            }
            parent_code = get_parent_code(self.naics_code)
            if parent_code:
                requirements['parent'] = QCEWColumns(naics_code=parent_code) 
    
            return requirements
        
        def columns(self):
            cols = OrderedDict()
            code, name, description = self.naics_code, NAICS_CODES[self.naics_code], ''
            
            # This gives us easier access to the tags we defined as dependencies
            input_ = self.input()
            units = input_['units']
            sections = input_['sections']
            subsections = input_['subsections']
            parent = input_.get('parent')
            cols['avg_wkly_wage'] = OBSColumn(
                # Make sure the column ID is unique within this module
                # If left blank, will be taken from this column's key in the output OrderedDict
                id=underscore_slugify(u'avg_wkly_wage_{}'.format(code)),
                # The PostgreSQL type of this column.  Generally Numeric for numbers and Text
                # for categories.
                type='Numeric',
                # Human-readable name.  Will be used as header in the catalog
                name=u'Average weekly wage for {} establishments'.format(name),
                # Human-readable description.  Will be used as content in the catalog.
                description=u'Average weekly wage for a given quarter in the {name} industry (NAICS {code}).'
                            u'{name} is {description}.'.format(name=name, code=code, description=description),
                # Ranking of importance, sometimes used to favor certain measures in auto-selection
                # Weight of 0 will hide this column from the user.  We generally use between 0 and 10
                weight=5,
                # How this measure was derived, for example "sum", "median", "average", etc.
                # In cases of "sum", this means functions downstream can construct estimates
                # for arbitrary geographies
                aggregate='average',
                # Tags are our way of noting aspects of this measure like its unit, the country
                # it's relevant to, and which section(s) of the catalog it should appear in.
                tags=[units['money'], sections['united_states'], subsections['income']],
            )
            cols['qtrly_estabs'] = OBSColumn(
                id=underscore_slugify(u'qtrly_estabs_{}'.format(code)),
                type='Numeric',
                name=u'Establishments in {}'.format(name),
                description=u'Count of establishments in a given quarter in the {name} industry (NAICS {code}).'
                            u'{name} is {description}.'.format(name=name, code=code, description=description),
                weight=5,
                aggregate='sum',
                tags=[units['businesses'], sections['united_states'], subsections['commerce_economy']],
                targets={parent['qtrly_estabs']: DENOMINATOR} if parent else {},
            )
            cols['month3_emplvl'] = OBSColumn(
                id=underscore_slugify(u'month3_emplvl_{}'.format(code)),
                type='Numeric',
                name=u'Employees in {} establishments'.format(name),
                description=u'Number of employees in the third month of a given quarter with the {name} '
                            u'industry (NAICS {code}). {name} is {description}.'.format(
                                name=name, code=code, description=description),
                weight=5,
                aggregate='sum',
                tags=[units['people'], sections['united_states'], subsections['employment']],
            )
            cols['lq_avg_wkly_wage'] = OBSColumn(
                id=underscore_slugify(u'lq_avg_wkly_wage_{}'.format(code)),
                type='Numeric',
                name=u'Average weekly wage location quotient for {} establishments'.format(name),
                description=u'Location quotient of the average weekly wage for a given quarter relative to '
                            u'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                            u'{name} is {description}.'.format(name=name, code=code, description=description),
                weight=3,
                aggregate=None,
                tags=[units['ratio'], sections['united_states'], subsections['income']],
            )
            cols['lq_qtrly_estabs'] = OBSColumn(
                id=underscore_slugify(u'lq_qtrly_estabs_{}'.format(code)),
                type='Numeric',
                name=u'Location quotient of establishments in {}'.format(name),
                description=u'Location quotient of the quarterly establishment count relative to '
                            u'the U.S. (Rounded to the hundredths place) within the {name} industry (NAICS {code}).'
                            u'{name} is {description}.'.format(name=name, code=code, description=description),
                weight=3,
                aggregate=None,
                tags=[units['ratio'], sections['united_states'], subsections['commerce_economy']],
            )
            cols['lq_month3_emplvl'] = OBSColumn(
                id=underscore_slugify(u'lq_month3_emplvl_{}'.format(code)),
                type='Numeric',
                name=u'Employment level location quotient in {} establishments'.format(name),
                description=u'Location quotient of the employment level for the third month of a given quarter '
                            u'relative to the U.S. (Rounded to the hundredths place) within the {name} '
                            u'industry (NAICS {code}). {name} is {description}.'.format(
                                name=name, code=code, description=description),
                weight=3,
                aggregate=None,
                tags=[units['ratio'], sections['united_states'], subsections['employment']],
            )
            return cols

We should never run metadata tasks on their own -- they should be
defined as requirements by ``TableTask``, below -- but it is possible to
do so, as an example.

NAICS code '1025' is the supersector for eduction & health.

.. code:: python

    education_health_columns = QCEWColumns(naics_code='1025')
    runtask(education_health_columns)
    education_health_columns.complete()




.. parsed-literal::

    True



Output from a ``ColumnsTask`` is an ``OrderedDict`` with the columns
wrapped in ``ColumnTarget``\ s, which allow us to pass them around
without immediately committing them to the database.

.. code:: python

    education_health_columns.output()




.. parsed-literal::

    OrderedDict([('avg_wkly_wage', <tasks.util.ColumnTarget at 0x7f12a40eead0>),
                 ('qtrly_estabs', <tasks.util.ColumnTarget at 0x7f12a5831c50>),
                 ('month3_emplvl', <tasks.util.ColumnTarget at 0x7f12a5831090>),
                 ('lq_avg_wkly_wage', <tasks.util.ColumnTarget at 0x7f12a4338b10>),
                 ('lq_qtrly_estabs', <tasks.util.ColumnTarget at 0x7f12a4d1fb50>),
                 ('lq_month3_emplvl',
                  <tasks.util.ColumnTarget at 0x7f12a4525390>)])



We can check the ``OBSColumn`` table for evidence that our metadata has
been committed to disk, since we ran the task.

.. code:: python

    [(col.id, col.name) for col in session.query(OBSColumn)[:5]]




.. parsed-literal::

    [(u'tmp.avg_wkly_wage_10',
      u'Average weekly wage for Total, all industries establishments'),
     (u'tmp.qtrly_estabs_10', u'Establishments in Total, all industries'),
     (u'tmp.month3_emplvl_10',
      u'Employees in Total, all industries establishments'),
     (u'tmp.lq_avg_wkly_wage_10',
      u'Average weekly wage location quotient for Total, all industries establishments'),
     (u'tmp.lq_qtrly_estabs_10',
      u'Location quotient of establishments in Total, all industries')]



6. Populate output table
------------------------

Now that we have our data in a format similar to what we'll need, and
our metadata lined up, we can tie it together with a ``TableTask``.
Under the hood, ``TableTask`` handles the relational lifting between
columns and actual data, and assigns a hash number to the dataset.

Several methods must be overriden for ``TableTask`` to work:

-  ``version()``: a version control number, which is useful for forcing
   a re-run/overwrite without having to track down and delete output
   artifacts.

-  ``timespan()``: the timespan (for example, '2014', or '2012Q4') that
   identifies the date range or point-in-time for this table.

-  ``columns()``: an OrderedDict of (colname, ColumnTarget) pairs. This
   should be constructed by pulling the desired columns from required
   ``ColumnsTask`` classes.

-  ``populate()``: a method that should populate (most often via) INSERT
   the output table.

.. code:: python

    # Since we have a column ('area_fips') that is a shared reference to
    # geometries ('geom_ref') we have to import that column.
    from tasks.us.census.tiger import GeoidColumns
    
    class QCEW(TableTask):
        
        year = IntParameter()
        qtr = IntParameter()
        
        def version(self):
            return 1
        
        def requires(self):
            requirements = {
                'data': SimpleQCEW(year=self.year, qtr=self.qtr),
                'geoid_cols': GeoidColumns(),
                'naics': OrderedDict()
            }
            for naics_code, naics_name in NAICS_CODES.iteritems():
                # Only include the more general NAICS codes
                if is_supersector(naics_code) or is_sector(naics_code) or naics_code == '10':
                    requirements['naics'][naics_code] = QCEWColumns(naics_code=naics_code)
            return requirements
        
        def timespan(self):
            return '{year}Q{qtr}'.format(year=self.year, qtr=self.qtr)
        
        def columns(self):
            # Here we assemble an OrderedDict using our requirements to specify the
            # columns that go into this table.
            # The column name 
            input_ = self.input()
            cols = OrderedDict([
                ('area_fips', input_['geoid_cols']['county_geoid'])
            ])
            for naics_code, naics_cols in input_['naics'].iteritems():
                for key, coltarget in naics_cols.iteritems():
                    naics_name = NAICS_CODES[naics_code]
                    colname = underscore_slugify(u'{}_{}_{}'.format(
                            key, naics_code, naics_name))
                    cols[colname] = coltarget
            return cols
        
        def populate(self):
            # This select statement transforms the input table, taking advantage of our
            # new column names.
            # The session is automatically committed if there are no errors.
            session = current_session()
            columns = self.columns()
            colnames = columns.keys()
            select_colnames = []
            for naics_code, naics_columns in self.input()['naics'].iteritems():
                for colname, coltarget in naics_columns.iteritems():
                    select_colnames.append('''MAX(CASE
                        WHEN industry_code = '{naics_code}' THEN {colname} ELSE NULL
                    END)::Numeric'''.format(naics_code=naics_code,
                                colname=colname
                              ))
            insert = '''INSERT INTO {output} ({colnames})
                        SELECT area_fips, {select_colnames}
                        FROM {input}
                        GROUP BY area_fips '''.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                            colnames=', '.join(colnames),
                            select_colnames=', '.join(select_colnames),
                        )
            session.execute(insert)

On a fresh database, this should return False Will not run if it has
been run before for this year & quarter combination.

.. code:: python

    table_task = QCEW(year=2014, qtr=4)
    table_task.complete()




.. parsed-literal::

    True



.. code:: python

    runtask(table_task)

The table should exist in metadata, as well as in data, with all
relations well-defined.

Unlike the ``TempTableTask``\ s above, the output of a ``TableTask`` is
a postgrse table in the ``observatory`` schema, with a unique hash name.

.. code:: python

    table = table_task.output()
    table.table




.. parsed-literal::

    'observatory.obs_3dc49b70f71ed9bbf5b4a48773c860519af70e1e'



It's possible for us to peek at the output data.

.. code:: python

    session.execute('SELECT * FROM {} LIMIT 1'.format(table.table)).fetchall()




.. parsed-literal::

    [(u'01001', None, None, None, None, None, None, Decimal('395'), Decimal('5'), Decimal('144'), Decimal('0.65'), Decimal('0.52'), Decimal('0.68'), Decimal('609'), Decimal('80'), Decimal('1024'), Decimal('0.96'), Decimal('0.66'), Decimal('0.74'), Decimal('364'), Decimal('68'), Decimal('368'), Decimal('0.79'), Decimal('0.95'), Decimal('1.13'), Decimal('917'), Decimal('3'), Decimal('66'), Decimal('0.68'), Decimal('0.94'), Decimal('1.00'), Decimal('2317'), Decimal('5'), Decimal('103'), Decimal('1.89'), Decimal('3.26'), Decimal('2.45'), Decimal('914'), Decimal('77'), Decimal('426'), Decimal('1.17'), Decimal('1.16'), Decimal('0.90'), Decimal('1231'), Decimal('33'), Decimal('157'), Decimal('1.26'), Decimal('0.60'), Decimal('0.35'), Decimal('925'), Decimal('20'), Decimal('198'), Decimal('1.14'), Decimal('1.66'), Decimal('1.30'), Decimal('914'), Decimal('77'), Decimal('426'), Decimal('1.17'), Decimal('1.16'), Decimal('0.90'), Decimal('1225'), Decimal('30'), Decimal('1347'), Decimal('1.45'), Decimal('1.01'), Decimal('1.44'), Decimal('584'), Decimal('85'), Decimal('1168'), Decimal('0.93'), Decimal('0.65'), Decimal('0.73'), Decimal('904'), Decimal('91'), Decimal('380'), Decimal('0.98'), Decimal('0.61'), Decimal('0.25'), None, None, None, None, None, None, Decimal('433'), Decimal('149'), Decimal('1935'), Decimal('1.13'), Decimal('1.63'), Decimal('1.57'), Decimal('1225'), Decimal('30'), Decimal('1347'), Decimal('1.45'), Decimal('1.01'), Decimal('1.44'), Decimal('274'), Decimal('66'), Decimal('1432'), Decimal('1.10'), Decimal('1.13'), Decimal('1.50'), Decimal('301'), Decimal('8'), Decimal('66'), Decimal('0.53'), Decimal('0.68'), Decimal('0.44'), Decimal('620'), Decimal('15'), Decimal('127'), Decimal('0.97'), Decimal('0.73'), Decimal('0.36'), None, None, None, None, None, None, Decimal('929'), Decimal('17'), Decimal('132'), Decimal('2.13'), Decimal('1.91'), Decimal('1.53'), Decimal('677'), Decimal('768'), Decimal('8173'), Decimal('0.97'), Decimal('0.95'), Decimal('0.91'), Decimal('364'), Decimal('68'), Decimal('368'), Decimal('0.79'), Decimal('0.95'), Decimal('1.13'), Decimal('275'), Decimal('74'), Decimal('1498'), Decimal('0.94'), Decimal('1.06'), Decimal('1.35'), Decimal('584'), Decimal('202'), Decimal('2322'), Decimal('1.01'), Decimal('1.20'), Decimal('1.12'), Decimal('781'), Decimal('114'), Decimal('430'), Decimal('0.70'), Decimal('1.55'), Decimal('0.72'), Decimal('1201'), Decimal('7'), Decimal('36'), Decimal('1.02'), Decimal('0.52'), Decimal('0.17'), Decimal('0'), Decimal('3'), Decimal('0'), Decimal('0'), Decimal('0.57'), Decimal('0'), Decimal('860'), Decimal('62'), Decimal('190'), Decimal('0.69'), Decimal('0.62'), Decimal('0.29'), Decimal('0'), Decimal('26'), Decimal('0'), Decimal('0'), Decimal('0.59'), Decimal('0'), Decimal('1201'), Decimal('7'), Decimal('36'), Decimal('1.02'), Decimal('0.52'), Decimal('0.17'), None, None, None, None, None, None, Decimal('628'), Decimal('41'), Decimal('122'), Decimal('0.87'), Decimal('1.28'), Decimal('0.77'), Decimal('842'), Decimal('73'), Decimal('308'), Decimal('0.67'), Decimal('1.75'), Decimal('0.71'))]

