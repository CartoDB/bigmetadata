from tasks.hierarchy import (DenormalizedHierarchy, Hierarchy, HierarchyChildParentsUnion, HierarchyInfoUnion,
                             HierarchyChildParent, GetParentsFunction, LevelHierarchy, LevelInfo)
from tasks.ca.statcan.geo import (GEO_PR, GEO_CD, GEO_FSA, GEO_CMA, GEO_CSD, GEO_DA, GEO_CT, GEO_DB,
                                  Geography)
from lib.logger import get_logger

LOGGER = get_logger(__name__)
COUNTRY = 'ca'


def _levels():
    return [GEO_DB, GEO_CT, GEO_DA, GEO_CSD, GEO_CMA, GEO_FSA, GEO_CD, GEO_PR]


class CADenormalizedHierarchy(DenormalizedHierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        return {
            'data': CAHierarchy(year=self.year),
            'function': CAGetParentsFunction(year=self.year),
            'rel': CAHierarchyChildParentsUnion(year=self.year, levels=levels)
        }


class CAGetParentsFunction(GetParentsFunction):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        return {
            'data': CAHierarchy(year=self.year),
            'rel': CAHierarchyChildParentsUnion(year=self.year, levels=levels),
            'info': CAHierarchyInfoUnion(year=self.year, levels=levels),
        }


class CAHierarchy(Hierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        LOGGER.debug('Levels: {}'.format(levels))
        return {
            'info': CAHierarchyInfoUnion(year=self.year, levels=levels),
            'rel': CAHierarchyChildParentsUnion(year=self.year, levels=levels)
        }


class CAHierarchyInfoUnion(HierarchyInfoUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        return [CALevelInfo(year=self.year, geography=level)
                for level in self.levels]


class CAHierarchyChildParentsUnion(HierarchyChildParentsUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        child_parents = self._child_parents()
        LOGGER.debug('Child-parents: {}'.format(child_parents))
        return {
            'hierarchy': [
                CAHierarchyChildParent(year=self.year,
                                       current_geography=child_parent[0],
                                       parent_geographies=child_parent[1])
                for child_parent in child_parents]
        }


class CAHierarchyChildParent(HierarchyChildParent):
    def requires(self):
        return {
            'level': CALevelHierarchy(year=self.year,
                                      current_geography=self.current_geography,
                                      parent_geographies=self.parent_geographies),
            'current_geom': Geography(year=self.year,
                                      resolution=self.current_geography),
            'parent_geoms': [Geography(year=self.year,
                                       resolution=parent_geography) for
                             parent_geography in self.parent_geographies]
        }

    @property
    def _current_geoid_field(self):
        return 'geom_id'

    @property
    def _parent_geoid_field(self):
        return 'geom_id'


class CALevelHierarchy(LevelHierarchy):
    def requires(self):
        return {
            'current_info': CALevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': Geography(year=self.year,
                                      resolution=self.current_geography),
            'parents_infos': [CALevelInfo(year=self.year,
                                          geography=parent_geography) for
                              parent_geography in self.parent_geographies],
            'parents_geoms': [Geography(year=self.year,
                                        resolution=parent_geography) for
                              parent_geography in self.parent_geographies]
        }

    @property
    def _geoid_field(self):
        return 'geom_id'


class CALevelInfo(LevelInfo):
    def requires(self):
        return Geography(year=self.year, resolution=self.geography)

    @property
    def _geoid_field(self):
        return 'geom_id'

    @property
    def _geoname_field(self):
        return 'geom_name'
