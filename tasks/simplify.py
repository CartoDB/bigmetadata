from luigi import WrapperTask, Parameter
import json
import os
from lib.logger import get_logger
from .simplification import SimplifyGeometriesMapshaper, SimplifyGeometriesPostGIS
from .meta import CurrentSession

TABLE_ID = 'tableid'
SIMPLIFICATION = 'simplification'
GEOM_FIELD = 'geomfield'
FACTOR = 'factor'
MAX_MEMORY = 'maxmemory'
SKIP_FAILURES = 'skipfailures'

OBSERVATORY_SCHEMA = 'observatory'
SIMPLIFICATION_MAPSHAPER = 'mapshaper'
SIMPLIFICATION_POSTGIS = 'postgis'

LOGGER = get_logger(__name__)


def find_table_name(table_id):
    session = CurrentSession().get()
    return session.execute("SELECT tablename FROM observatory.obs_table WHERE id = '{tableid}'".format(
                            tableid=table_id)).fetchone()[0]


class SimplifyOne(WrapperTask):
    table = Parameter()

    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
            tables = json.load(file)

            table = tables[self.table]
            tablename = find_table_name(self.table)
            LOGGER.info("Simplifying %s (%s) using %s", self.table, tablename, table.get(SIMPLIFICATION))

            simplification = table[SIMPLIFICATION]
            if simplification == SIMPLIFICATION_MAPSHAPER:
                return SimplifyGeometriesMapshaper(schema=OBSERVATORY_SCHEMA, table_input=tablename,
                                                   geomfield=table[GEOM_FIELD], retainfactor=table[FACTOR],
                                                   skipfailures=table[SKIP_FAILURES], maxmemory=table[MAX_MEMORY])
            elif simplification == SIMPLIFICATION_POSTGIS:
                return SimplifyGeometriesPostGIS(schema=OBSERVATORY_SCHEMA, table_input=tablename,
                                                 geomfield=table[GEOM_FIELD], retainfactor=table[FACTOR])
            else:
                raise ValueError('Invalid simplification "{simplification}" for {table}'.format(
                    simplification=simplification, table=self.table))


class SimplifyAll(WrapperTask):
    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
            return [SimplifyOne(table=table) for table in json.load(file)]
