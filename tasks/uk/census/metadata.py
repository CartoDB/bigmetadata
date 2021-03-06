from collections import OrderedDict
import json
import os
import re

from tasks.meta import OBSColumn, DENOMINATOR, OBSTag
from tasks.base_tasks import ColumnsTask, TagsTask
from tasks.tags import UnitTags, SectionTags, SubsectionTags, LicenseTags


def load_definition():
    with open(os.path.join(os.path.dirname(__file__), 'columns.json')) as json_file:
        return json.load(json_file, object_pairs_hook=OrderedDict)


COLUMNS_DEFINITION = load_definition()


DISALLOWED_CHARACTERS_RE = re.compile(r'[:/, \-\.\(\)]')
TABLE_CODE_PARTS_RE = re.compile('([A-Z]{2})\d+([A-Z]{2})[a-z]?')


def sanitize_identifier(colid):
    return DISALLOWED_CHARACTERS_RE.sub('_', '_'.join(colid.split(';')[0].split(':')[-2:]))


def parse_table(table_id):
    '''
    Returns a tuple like (KS, NI) from a table string like KS201NI
    '''
    return TABLE_CODE_PARTS_RE.match(table_id).groups()


class SourceTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='ons',
                       name='Office for National Statistics (ONS)',
                       type='source',
                       description="The UK's largest independent producer of official statistics and the recognised national statistical institute of the UK (`ONS <https://www.ons.gov.uk/>`_)"),
                OBSTag(id='scotland-census',
                       name="Scotland's Census Data Warehouse by National Records of Scotland",
                       type='source',
                       description="`Scotland Census <http://www.scotlandscensus.gov.uk/`_"),
                OBSTag(id='nisra',
                       name="Northern Ireland Statistics and Research Agency",
                       type='source',
                       description="`Northern Ireland Statistics and Research Agency <https://www.nisra.gov.uk/`_")]


class CensusColumns(ColumnsTask):
    def requires(self):
        return {
            'units': UnitTags(),
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def version(self):
        return 5

    def prefix(self):
        return 'uk.ons'

    def columns(self):
        input_ = self.input()
        sources = list(input_['source'].values())
        license_tag = input_['license']['uk_ogl']
        uk = input_['sections']['uk']
        subsections = input_['subsections']
        units = input_['units']

        columns = OrderedDict()
        for key, column in COLUMNS_DEFINITION.items():
            columns[key] = OBSColumn(
                id=column['id'],
                name=column['name'],
                description='',
                type='Numeric',
                weight=column['weight'],
                aggregate='sum',
                targets={columns[denom]: DENOMINATOR for denom in column['denominators']},
                tags=sources + [uk, license_tag, units[column['units']], subsections[column['subsection']]]
            )

        return columns
