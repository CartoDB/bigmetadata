from collections import OrderedDict

from tasks.meta import OBSColumn, GEOM_REF
from tasks.util import ColumnsTask
from tasks.us.ny.nyc.tags import NYCTags

class NYCColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': NYCTags()
        }

    def version(self):
        return 5

    def columns(self):
        input_ = self.input()
        nyc_tags = input_['tags']
        parcel = OBSColumn(
            type="Geometry",
            name="",
            weight=1,
            description="",
        )
        cols = OrderedDict([
            ('borough', OBSColumn(
                type='TEXT',
                weight=1,
                name='Borough')
            ),
            ('bin', OBSColumn(
                type='TEXT',
                weight=1,
                name='Building identification number')
            ),
            ('bbl', OBSColumn(
                type='TEXT',
                name='Borough block & lot',
                weight=1,
                targets={parcel: GEOM_REF})
            ),
            ('parcel', parcel),
            ('block', OBSColumn(
                type='TEXT',
                weight=1,
                name='Building block')
            ),
            ('lot', OBSColumn(
                type='TEXT',
                weight=1,
                name='Building lot')
            ),
            ('cd', OBSColumn(
                weight=1,
                type='TEXT',
                name='Community District')
            ),
            ("tract2010", OBSColumn(
                type='TEXT',
                weight=1,
                name='Tract code 2010')
            ),
            ("ct2010", OBSColumn(
                type='Numeric',
                weight=1,
                name='Tract code 2010')
            ),
            ("cb2010", OBSColumn(
                type='Numeric',
                weight=1,
                name='Block code 2010')
            ),
            ("schooldist", OBSColumn(
                type='TEXT',
                weight=1,
                name='School District')
            ),
            ("council", OBSColumn(
                type='TEXT',
                weight=1,
                name='City Council District')
            ),
            ("firecomp", OBSColumn(
                type='TEXT',
                weight=1,
                name='Fire Company')
            ),
            ("policeprct", OBSColumn(
                type='TEXT',
                weight=1,
                name='Police Precinct')
            ),
            ("healtharea", OBSColumn(
                type='TEXT',
                weight=1,
                name='Health area')
            ),
            ("sanitdistr", OBSColumn(
                type='TEXT',
                weight=1,
                name='Sanitation district')
            ),
            ("sanitboro", OBSColumn(
                type='TEXT',
                weight=1,
                name='Borough of sanitation district')
            ),
            ("sanitsub", OBSColumn(
                type='TEXT',
                weight=1,
                name='Sanitation district subsection')
            ),
            ("borocode", OBSColumn(
                type='Numeric',
                weight=1,
                name='Borough code')
            )
        ])
        return cols

    def tags(self, input_, col_key, col):
        return input_['tags']['nyc']
