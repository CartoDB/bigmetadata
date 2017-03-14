from collections import OrderedDict

from tasks.meta import OBSColumn
from tasks.util import ColumnsTask


class POIColumns(ColumnsTask):

    def version(self):
        return 3

    def columns(self):
        return OrderedDict([
            ('house_number', OBSColumn(type='Text',
                                       name='House number'
                                      )),
            ('street_name', OBSColumn(type='Text',
                                      name='Street name'
                                     )),
            ('postal_code', OBSColumn(type='Text',
                                      name='Postal code'
                                     )),
            ('address', OBSColumn(type='Text',
                                  name='Address'
                                 )),
        ])
