'''
Define special segments for the census
'''


from tasks.meta import OBSTag
from tasks.tasks import TagsTask


class SegmentTags(TagsTask):

    def tags(self):

        return [
            OBSTag(id='middle_aged_men',
                   name='Middle Aged Men (45 to 64 years old)',
                   type='segment',
                   description=''),
            OBSTag(id='families_with_young_children',
                   name='Families with young children (Under 6 years old)',
                   type='segment',
                   description='')
        ]
