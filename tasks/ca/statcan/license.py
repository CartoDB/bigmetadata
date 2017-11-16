from tasks.meta import OBSTag
from tasks.base_tasks import TagsTask


class SourceTags(TagsTask):

    def tags(self):
        return [
            OBSTag(id='statcan-nhs-2011',
                   name='Statistics Canada National Household Survey 2011',
                   type='source',
                   description='Adapted from Statistics Canada, National Household Survey 2011. This does not constitute an endorsement by Statistics Canada of this product.'),
            OBSTag(id='statcan-census-2011',
                   name='Statistics Canada Census of Population 2011',
                   type='source',
                   description='Adapted from Statistics Canada, Census of Population 2011. This does not constitute an endorsement by Statistics Canada of this product.'),
               ]


class LicenseTags(TagsTask):

    def tags(self):
        return [OBSTag(id='statcan-license',
                       name='Statistics Canada Open Licence Agreement',
                       type='license',
                       description='Further details `here <http://www.statcan.gc.ca/eng/reference/licence>`_') ]
