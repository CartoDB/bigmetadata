import os
import json
from collections import OrderedDict
from tasks.base_tasks import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags, PublicTags
from tasks.ca.statcan.license import LicenseTags, SourceTags
from tasks.meta import OBSColumn, DENOMINATOR, OBSTag


def load_definition():
    with open(os.path.join(os.path.dirname(__file__), 'census_columns.json')) as json_file:
        return json.load(json_file, object_pairs_hook=OrderedDict)


COLUMNS_DEFINITION = load_definition()
DEFAULT_WEIGHT = 10
DEFAULT_AGGREGATE = 'sum'
GENDERS = {
    'f': 'Female',
    'm': 'Male',
}


class CensusColumns(ColumnsTask):

    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
            'license': LicenseTags(),
            'source': SourceTags(),
            'public': PublicTags()
        }

    def version(self):
        return 1

    def columns(self):
        input_ = self.input()

        license_ca = input_['license']['statcan-license']
        source_ca = input_['source']['statcan-census-2016']
        section = input_['sections']['ca']
        subsections = input_['subsections']
        public = input_['public']['public']

        units = input_['units']

        cols = OrderedDict()

        for key, column in COLUMNS_DEFINITION.items():
            tags = [source_ca, section, public, license_ca, subsections[column['subsection']]]
            tags += [units[column.get('units')]] if column.get('units') is not None else []

            cols[key + '_t'] = OBSColumn(
                id=column['id'] + '_t',
                name=column['name'],
                description=column.get('description', ''),
                type='Numeric',
                weight=column.get('weight', DEFAULT_WEIGHT),
                aggregate=column.get('aggregate', DEFAULT_AGGREGATE),
                targets={cols[denom + '_t']: DENOMINATOR for denom in column['denominators']},
                tags=tags
            )
            if column.get('gender_split', 'no').lower() == 'yes':
                for suffix, gender in GENDERS.items():
                    denominators = [denom + '_t' for denom in column['denominators']] + \
                                   [denom + suffix for denom in column['denominators']]

                    cols[key + suffix] = OBSColumn(
                        id='{}_{}'.format(column['id'], suffix),
                        name='{} ({})'.format(column['name'], gender),
                        description=column.get('description', ''),
                        type='Numeric',
                        weight=column.get('weight', DEFAULT_WEIGHT),
                        aggregate=column.get('aggregate', DEFAULT_AGGREGATE),
                        targets={cols[denom]: DENOMINATOR for denom in denominators},
                        tags=tags
                    )

        return cols
