from lib.logger import get_logger
from tasks.hierarchy import DenormalizedHierarchy, GetParentsFunction, \
    Hierarchy, HierarchyInfoUnion, HierarchyChildParentsUnion, \
    HierarchyChildParent, LevelHierarchy, LevelInfo
from tasks.uk.cdrc import OutputAreas
from tasks.uk.datashare import PostcodeAreas
from tasks.uk.gov import LowerLayerSuperOutputAreas, \
    MiddleLayerSuperOutputAreas
from tasks.uk.odl import PostcodeSectors, PostcodeDistricts

LOGGER = get_logger(__name__)

COUNTRY = 'uk'

LEVEL_CLASSES = {
    'uk.cdrc.the_geom': OutputAreas,
    'uk.odl.ps_geo': PostcodeSectors,
    'uk.gov.lsoa_geo': LowerLayerSuperOutputAreas,
    'uk.gov.msoa_geo': MiddleLayerSuperOutputAreas,
    'uk.odl.pd_geo': PostcodeDistricts,
    'uk.datashare.pa_geo': PostcodeAreas
}

LEVELS = list(LEVEL_CLASSES.keys())


def geography(level):
    return lambda year: LEVEL_CLASSES[level]()


class UKTask:
    @property
    def _country(self):
        COUNTRY


class UKDenormalizedHierarchy(UKTask, DenormalizedHierarchy):
    def requires(self):
        return {
            'data': UKHierarchy(year=self.year),
            'function': UKGetParentsFunction(year=self.year),
            'rel': UKHierarchyChildParentsUnion(year=self.year, levels=LEVELS)
        }


class UKGetParentsFunction(UKTask, GetParentsFunction):
    def requires(self):
        return {
            'data': UKHierarchy(year=self.year),
            'rel': UKHierarchyChildParentsUnion(year=self.year, levels=LEVELS),
            'info': UKHierarchyInfoUnion(year=self.year, levels=LEVELS)
        }


class UKHierarchy(UKTask, Hierarchy):
    def requires(self):
        return {
            'rel': UKHierarchyChildParentsUnion(year=self.year, levels=LEVELS),
            'info': UKHierarchyInfoUnion(year=self.year, levels=LEVELS)
        }


class UKHierarchyInfoUnion(UKTask, HierarchyInfoUnion):
    def requires(self):
        return [UKLevelInfo(year=self.year, geography=level)
                for level in self.levels]


class UKHierarchyChildParentsUnion(UKTask, HierarchyChildParentsUnion):
    def requires(self):
        child_parents = self._child_parents()
        return {
            'hierarchy': [
                UKHierarchyChildParent(year=self.year,
                                       current_geography=child_parent[0],
                                       parent_geographies=child_parent[1])
                for child_parent in child_parents]
        }


class UKHierarchyChildParent(HierarchyChildParent):
    def requires(self):
        return {
            'level': UKLevelHierarchy(year=self.year,
                                      current_geography=self.current_geography,
                                      parent_geographies=self.parent_geographies),
            'current_geom': geography(level=self.current_geography)(
                year=self.year),
            'parent_geoms': [
                geography(level=parent_geography)(year=self.year) for
                parent_geography in self.parent_geographies]
        }

    @property
    def _current_geoid_field(self):
        return self.input()['current_geom'].geoid_column()

    @property
    def _parent_geoid_field(self):
        return self.input()['parent_geom'].geoid_column()


class UKLevelHierarchy(LevelHierarchy):
    def requires(self):
        return {
            'current_info': UKLevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': geography(level=self.current_geography)(
                year=self.year),
            'parents_infos': [UKLevelInfo(year=self.year,
                                          geography=parent_geography) for
                              parent_geography in self.parent_geographies],
            'parents_geoms': [
                geography(level=parent_geography)(year=self.year) for
                parent_geography in self.parent_geographies]
        }

    @property
    def _geoid_field(self):
        return 'geom_id'


class UKLevelInfo(LevelInfo):
    def requires(self):
        return geography(level=self.geography)(year=self.year)

    @property
    def _geoid_field(self):
        return self.input().geoid_column()

    @property
    def _geoname_field(self):
        return self.input().geoname_column()
