## Reusing functions and classes in development

Writing ETL tasks is pretty repetitive.  In `tasks/util.py` are a number of
functions and classes that are meant to make life easier through reusability:

### Functions

* `shell(cmd)`

* `query_cartodb(query)`

* `import_api(request, json_column_names=None)`

#### Honorable function mentions

* `underscore_slugify(txt)`

* `classpath(obj)`

* `sql_to_cartodb_table(outname, localname, json_column_names=None,
                        schema='observatory')

### Classes

* `TempTableTask`

* `ColumnsTask`

* `TagsTask`

#### Honorable class mentions

* `PostgresTarget`

* `CartoDBTarget`
