from tasks.base_tasks import TempTableTask
from tasks.uk.gov import LowerLayerSuperOutputAreas
from tasks.meta import current_session


class Shoreline(TempTableTask):

    def requires(self):
        return LowerLayerSuperOutputAreas()

    def version(self):
        return 1

    def run(self):
        session = current_session()

        stmt = '''
                CREATE TABLE {output_table} AS
                SELECT ST_Union(the_geom) the_geom
                  FROM {input_table}
               '''.format(
                   output_table=self.output().table,
                   input_table=self.input().table,
               )

        session.execute(stmt)
