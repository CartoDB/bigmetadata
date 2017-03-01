from collections import OrderedDict

from tasks.meta import OBSColumn
from tasks.util import ColumnsTask

class NYCColumns(ColumnsTask):

    def version(self):
        return 2

    def columns(self):
        return OrderedDict([
            ('borough', OBSColumn(
                type='TEXT',
                name='Borough')
            ),
            ('bin', OBSColumn(
                type='TEXT',
                name='Building identification number')
            ),
            ('bbl', OBSColumn(
                type='TEXT',
                name='Borough block & lot')
            ),
            ('block', OBSColumn(
                type='TEXT',
                name='Building block')
            ),
            ('lot', OBSColumn(
                type='TEXT',
                name='Building lot')
            ),
            ('cd', OBSColumn(
                type='TEXT',
                name='Community District')
            ),
            ("tract2010", OBSColumn(
                type='TEXT',
                name='Tract code 2010')
            ),
            ("ct2010", OBSColumn(
                type='Numeric',
                name='Tract code 2010')
            ),
            ("cb2010", OBSColumn(
                type='Numeric',
                name='Block code 2010')
            ),
            ("schooldist", OBSColumn(
                type='TEXT',
                name='School District')
            ),
            ("council", OBSColumn(
                type='TEXT',
                name='City Council District')
            ),
            ("firecomp", OBSColumn(
                type='TEXT',
                name='Fire Company')
            ),
            ("policeprct", OBSColumn(
                type='TEXT',
                name='Police Precinct')
            ),
            ("healtharea", OBSColumn(
                type='TEXT',
                name='Health area')
            ),
            ("sanitdistr", OBSColumn(
                type='TEXT',
                name='Sanitation district')
            ),
            ("sanitboro", OBSColumn(
                type='TEXT',
                name='Borough of sanitation district')
            ),
            ("sanitsub", OBSColumn(
                type='TEXT',
                name='Sanitation district subsection')
            ),
            ("borocode", OBSColumn(
                type='Numeric',
                name='Borough code')
            )
        ])
