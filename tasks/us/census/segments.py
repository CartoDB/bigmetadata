'''
Define special segments for the census
'''


from tasks.meta import BMDTag, BMDColumn
from tasks.util import TagsTask, ColumnsTask, TableTask


class SegmentTags(TagsTask):

    def tags(self):

        return [
            BMDTag(id='middle_aged_men',
                   name='Middle Aged Men (45 to 64 years old)',
                   description=''),
            BMDTag(id='families_with_young_children',
                   name='Families with young children (Under 6 years old)',
                   description='')
        ]


class SegmentColumns(ColumnsTask):

    def columns(self):

        pass


class SegmentTable(TableTask):
    pass
