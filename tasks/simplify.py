from luigi import WrapperTask, Parameter
import json
import os
from lib.logger import get_logger
from .simplification import SimplifyGeometriesMapshaper, SimplifyGeometriesPostGIS

SCHEMA = 'schema'
TABLE_ID = 'tableid'
SIMPLIFICATION = 'simplification'
GEOM_FIELD = 'geomfield'
FACTOR = 'factor'
MAX_MEMORY = 'maxmemory'
SKIP_FAILURES = 'skipfailures'

SIMPLIFICATION_MAPSHAPER = 'mapshaper'
SIMPLIFICATION_POSTGIS = 'postgis'

LOGGER = get_logger(__name__)


def get_simplification_params(table_id):
    with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
        return json.load(file).get(table_id)


class Simplify(WrapperTask):
    schema = Parameter()
    table = Parameter()
    table_id = Parameter(default='')

    def __init__(self, *args, **kwargs):
        super(Simplify, self).__init__(*args, **kwargs)

        self.table_key = self.table
        if self.table_id:
            self.table_key = self.table_id

    def requires(self):
        params = get_simplification_params(self.table_key.lower())
        if not params:
            LOGGER.error("Simplification not found. Edit 'simplifications.json' and add an entry for '{}'".format(self.table_key.lower()))
        LOGGER.info("Simplifying %s using %s", self.table, params.get(SIMPLIFICATION))

        simplification = params[SIMPLIFICATION]
        if simplification == SIMPLIFICATION_MAPSHAPER:
            return SimplifyGeometriesMapshaper(schema=self.schema, table_input=self.table,
                                               geomfield=params[GEOM_FIELD], retainfactor=params[FACTOR],
                                               skipfailures=params[SKIP_FAILURES], maxmemory=params[MAX_MEMORY])
        elif simplification == SIMPLIFICATION_POSTGIS:
            return SimplifyGeometriesPostGIS(schema=self.schema, table_input=self.table,
                                             geomfield=params[GEOM_FIELD], retainfactor=params[FACTOR])
        else:
            raise ValueError('Invalid simplification "{simplification}" for {table}'.format(
                simplification=simplification, table=self.table))
