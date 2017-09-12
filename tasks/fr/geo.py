from tasks.util import (MetaWrapper, Shp2TempTableTask, TableTask, ColumnsTask, DownloadUnzipTask, shell)
from tasks.meta import current_session, GEOM_REF, GEOM_NAME
from collections import OrderedDict
import os
from tasks.meta import OBSColumn
from tasks.tags import SectionTags, SubsectionTags, BoundaryTags


class DownloadOutputAreas(DownloadUnzipTask):
    # Note that this set of IRIS contours is from 2013, may need to find 2014 contours to match the data

    URL = 'https://www.data.gouv.fr/s/resources/contour-des-iris-insee-tout-en-un/20150428-161348/iris-2013-01-01.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(output=self.output().path, url=self.URL))


class ImportOutputAreas(Shp2TempTableTask):

    def requires(self):
        return DownloadOutputAreas()

    def input_shp(self):
        # may need to point to directory iris-2013-01-01?
        return os.path.join(self.input().path, 'iris-2013-01-01.shp')


class OutputAreaColumns(ColumnsTask):

    def version(self):
        return 4

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'boundary_type': BoundaryTags()
        }

    def columns(self):
        input_ = self.input()

        geom = OBSColumn(
            type='Geometry',
            name='IRIS and Commune areas',
            description='IRIS regions are defined by INSEE census for purposes of all municipalities '
                        'of over 10000 inhabitants and most towns from 5000 to 10000. For areas in which '
                        'IRIS is not defined, the commune area is given instead. ',
            weight=5,
            tags=[input_['subsections']['boundary'], input_['sections']['fr'],
                  input_['boundary_type']['interpolation_boundary'],
                  input_['boundary_type']['cartographic_boundary'],
                  ]
        )
        geomref = OBSColumn(
            type='Text',
            name='DCOMIRIS',
            description='Full Code IRIS. Result of the concatenation of DEPCOM and IRIS attributes. ',
            weight=0,
            targets={geom: GEOM_REF}
        )
        commune_name = OBSColumn(
            type='Text',
            name='Name of Commune',
            description='Name of the commune. ',
            weight=1,
            tags=[input_['subsections']['names'], input_['sections']['fr']],
            targets={geom: GEOM_NAME}
        )
        iris_name = OBSColumn(
            type='Text',
            name='Name of IRIS',
            description='Name of the IRIS. This attribute may possibly be unfilled. '
            'For small undivided towns, the name of the IRIS is the name of the commune. ',
            weight=1,
            tags=[input_['subsections']['names'], input_['sections']['fr']],
            targets={geom: GEOM_NAME}
        )
        return OrderedDict([
            ('the_geom', geom),
            ('dcomiris', geomref),
            ('nom_com', commune_name),
            ('nom_iris', iris_name),
        ])


class OutputAreas(TableTask):

    def requires(self):
        return {
            'geom_columns': OutputAreaColumns(),
            'data': ImportOutputAreas(),
        }

    def version(self):
        return 4

    def timespan(self):
        return 2013

    def columns(self):
        input_ = self.input()
        cols = OrderedDict()
        cols.update(input_['geom_columns'])
        return cols

    def populate(self):
        session = current_session()
        session.execute('INSERT INTO {output} '
                        'SELECT DISTINCT ST_MakeValid(wkb_geometry), DCOMIRIS, NOM_COM, NOM_IRIS '
                        'FROM {input}'.format(
                            output=self.output().table,
                            input=self.input()['data'].table,
                        ))


class AllGeo(MetaWrapper):

    def requires(self):
        yield OutputAreas()
