import os
import json
import csv

from luigi import Task, WrapperTask, Parameter
from luigi.local_target import LocalTarget
from tasks.meta import current_session
from tasks.uk.census.wrapper import CensusPostcodeAreas, CensusPostcodeDistricts, CensusPostcodeSectors
from lib.logger import get_logger

LOGGER = get_logger(__name__)

GEOGRAPHY_LEVELS = {
    'GEO_PA': 'uk.datashare.pa_geo',  # Postcode Areas
    'GEO_PD': 'uk.odl.pd_geo',  # Postcode Districts
    'GEO_PS': 'uk.odl.ps_geo',  # Postcode Sectors
}


class Measurements2CSV(Task):
    geography = Parameter()
    file_name = Parameter()

    def __init__(self, *args, **kwargs):
        super(Measurements2CSV, self).__init__(*args, **kwargs)

    def requires(self):
        requirements = {}
        if self.geography == 'GEO_PA':
            requirements['data'] = CensusPostcodeAreas()
        elif self.geography == 'GEO_PD':
            requirements['data'] = CensusPostcodeDistricts()
        elif self.geography == 'GEO_PS':
            requirements['data'] = CensusPostcodeSectors()

        return requirements

    def _get_config_data(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with (open('{}/{}'.format(dir_path, 'measurements.json'))) as f:
            return json.load(f)

    def run(self):
        session = current_session()
        measurements = self._get_config_data()
        for measure in measurements:
            measure['geom_id'] = GEOGRAPHY_LEVELS[self.geography]
        json_metadata = json.dumps(measurements)

        result = session.execute(self._get_meta_query(json_metadata))
        if result:
            join_data = {}
            join_data['numer'] = {}
            if self.geography == 'GEO_PA':
                colnames = ['geom.pa_id as geoid']
            else:
                colnames = ['geom.geographycode as geoid']

            for data in result.fetchall():
                join_data['numer'][data['numer_table']] = {'table': 'observatory.{}'.format(data['numer_table']),
                                                           'join_column': data['numer_join_col']}
                # All come from the same geometry tables so we use, by now, just one geometry
                # TODO Make it possible to have multiple geometry tables
                join_data['geom'] = {'table': 'observatory.{}'.format(data['geom_table']), 'join_column': data['geom_join_col']}
                colnames.append(data['numer_col'])

            measurement_result = session.execute(self._get_measurements_query(join_data, colnames))
            if measurement_result:
                measurements = measurement_result.fetchall()
                self._generate_csv_file(colnames, measurements)
            else:
                LOGGER.error('No results for the queried measurements')

        else:
            LOGGER.error('No results for the defined measurements in the JSON file')

    def _get_meta_query(self, metadata):
        return '''SELECT meta->>'numer_tablename' numer_table, meta->>'numer_geomref_colname' numer_join_col,
                         meta->>'numer_colname' numer_col, meta->>'geom_tablename' geom_table,
                         meta->>'geom_geomref_colname' geom_join_col, meta->>'geom_colname' geom_col
                  FROM json_array_elements(cdb_observatory.OBS_GetMeta(
                       ST_MakeEnvelope(-179, 89, 179, -89, 4326), -- World bbox
                       '{}'::json, 1, 1, 1)) meta
            '''.format(metadata)

    def _get_measurements_query(self, join_data, colnames):
        joins = []
        for join_table in join_data['numer'].values():
            joins.append('LEFT JOIN {table} ON (geom.{geomcol} = {table}.{numercol})'.format(
                table=join_table['table'], geomcol=join_data['geom']['join_column'],
                numercol=join_table['join_column']
            ))
        return '''SELECT {cols}
                  FROM {geom} geom {joins}
               '''.format(cols=' ,'.join(colnames), geom=join_data['geom']['table'],
                          joins=' '.join(joins))

    def _generate_csv_file(self, headers, measurements):
        try:
            self.output().makedirs()
            with(open(self.output().path, 'w+')) as csvfile:
                headers[0]='geoid'
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                for measurement in measurements:
                    writer.writerow(dict(measurement))
        except BaseException:
            self.output().remove

    def output(self):
        csv_filename = 'tmp/geographica/uk/{}'.format(self.file_name)
        return LocalTarget(path=csv_filename, format='csv')


class AllMeasurements(WrapperTask):
    def requires(self):
        for geography in GEOGRAPHY_LEVELS.keys():
            yield Measurements2CSV(geography=geography, file_name='do_uk_{}.csv'.format(geography))
