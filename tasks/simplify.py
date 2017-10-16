from luigi import Task, Parameter
import json
import os
from util import get_logger
from simplification import SimplifyGeometriesMapshaper, SimplifyGeometriesPostGIS


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


class SimplifyOne(Task):
    table = Parameter()

    def run(self):
        with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
            tables = json.load(file)

            if self.table in tables:
                table = tables.get(self.table)
                LOGGER.info("Simplifying %s (%s) using %s", self.table, table.get(TABLE_ID), table.get(SIMPLIFICATION))

                simplification = table.get(SIMPLIFICATION)
                if simplification == SIMPLIFICATION_MAPSHAPER:
                    yield SimplifyGeometriesMapshaper(schema=OBSERVATORY_SCHEMA, table_input=self.table,
                                                      geomfield=table[GEOM_FIELD], retainfactor=table[FACTOR],
                                                      skipfailures=table[SKIP_FAILURES], maxmemory=table[MAX_MEMORY])
                elif simplification == SIMPLIFICATION_POSTGIS:
                    yield SimplifyGeometriesPostGIS(schema=OBSERVATORY_SCHEMA, table_input=self.table,
                                                    geomfield=table[GEOM_FIELD], retainfactor=table[FACTOR])
                else:
                    raise ValueError('Invalid simplification "{simplification}" for {table}'.format(
                        simplification=simplification, table=self.table))


class SimplifyAll(Task):
    def requires(self):
        with open(os.path.join(os.path.dirname(__file__), 'simplifications.json')) as file:
            tables = json.load(file)

            tasks = []
            for table in tables:
                tasks.append(SimplifyOne(table=table))

            return tasks
