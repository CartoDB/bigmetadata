from luigi import WrapperTask, Parameter
import json
import os
from lib.logger import get_logger
from .simplification import SimplifyGeometriesMapshaper, \
    SimplifyGeometriesPostGIS, SIMPLIFIED_SUFFIX

SCHEMA = 'schema'
TABLE_ID = 'tableid'
SIMPLIFICATION = 'simplification'
GEOM_FIELD = 'geomfield'
FACTOR = 'factor'
MAX_MEMORY = 'maxmemory'
SKIP_FAILURES = 'skipfailures'

SIMPLIFICATION_MAPSHAPER = 'mapshaper'
SIMPLIFICATION_POSTGIS = 'postgis'

DEFAULT_SIMPLIFICATION = SIMPLIFICATION_MAPSHAPER
DEFAULT_MAXMEMORY = '8192'
DEFAULT_SKIPFAILURES = 'no'

LOGGER = get_logger(__name__)


def get_simplification_params(table_id):
    with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
        return json.load(file).get(table_id)


class Simplify(WrapperTask):
    schema = Parameter()
    table = Parameter()
    table_id = Parameter(default='')
    suffix = Parameter(default=SIMPLIFIED_SUFFIX)

    def __init__(self, *args, **kwargs):
        super(Simplify, self).__init__(*args, **kwargs)

        self.table_key = self.table
        if self.table_id:
            self.table_key = self.table_id

    def requires(self):
        params = get_simplification_params(self.table_key.lower())
        if not params:
            LOGGER.error("Simplification not found. Edit 'simplifications.json' and add an entry for '{}'".format(self.table_key.lower()))

        simplification = params.get(SIMPLIFICATION, DEFAULT_SIMPLIFICATION)
        LOGGER.info("Simplifying %s.%s using %s", self.schema, self.table, simplification)
        table_output = '{tablename}{suffix}'.format(tablename=self.table, suffix=self.suffix)
        if simplification == SIMPLIFICATION_MAPSHAPER:
            return SimplifyGeometriesMapshaper(schema=self.schema,
                                               table_input=self.table,
                                               table_output=table_output,
                                               geomfield=params[GEOM_FIELD],
                                               retainfactor=params[FACTOR],
                                               skipfailures=params.get(SKIP_FAILURES, DEFAULT_SKIPFAILURES),
                                               maxmemory=params.get(MAX_MEMORY, DEFAULT_MAXMEMORY))
        elif simplification == SIMPLIFICATION_POSTGIS:
            return SimplifyGeometriesPostGIS(schema=self.schema,
                                             table_input=self.table,
                                             table_output=table_output,
                                             geomfield=params[GEOM_FIELD],
                                             retainfactor=params[FACTOR])
        else:
            raise ValueError('Invalid simplification "{simplification}" for {table}'.format(
                simplification=simplification, table=self.table))
