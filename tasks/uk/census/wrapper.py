from tasks.util import MetaWrapper

from .ons import EnglandWalesLocal
from tasks.uk.cdrc import OutputAreas


class CensusWrapper(MetaWrapper):
    def tables(self):
        yield EnglandWalesLocal()
        yield OutputAreas()
