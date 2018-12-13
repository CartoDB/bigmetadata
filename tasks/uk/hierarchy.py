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
    'cdrc_the_geom': OutputAreas,
    'odl_ps_geo': PostcodeSectors,
    'gov_lsoa_geo': LowerLayerSuperOutputAreas,
    'gov_msoa_geo': MiddleLayerSuperOutputAreas,
    'odl_pd_geo': PostcodeDistricts,
    'datashare_pa_geo': PostcodeAreas
}

LEVELS = list(LEVEL_CLASSES.keys())


def geography(level):
    return lambda year: geography_class(level)()


def geography_class(level):
    return LEVEL_CLASSES[level]


class UKTask:
    @property
    def _country(self):
        return COUNTRY


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
                                      parent_geographies=self.parent_geographies,
                                      parent_geoid_fields=self._parent_geoid_fields),
            'current_geom': geography(level=self.current_geography)(
                year=self.year),
            'parent_geoms': [
                geography(level=parent_geography)(year=self.year) for
                parent_geography in self.parent_geographies]
        }

    @property
    def _current_geoid_field(self):
        return geography_class(self.current_geography).geoid_column()

    @property
    def _parent_geoid_fields(self):
        return [
            geography_class(parent_geography).geoid_column() for
            parent_geography in self.parent_geographies
        ]


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
        return geography_class(self.current_geography).geoid_column()


class UKLevelInfo(LevelInfo):
    def requires(self):
        return geography(level=self.geography)(year=self.year)

    @property
    def _geoid_field(self):
        return geography_class(self.geography).geoid_column()

    @property
    def _geoname_field(self):
        return geography_class(self.geography).geoname_column()
