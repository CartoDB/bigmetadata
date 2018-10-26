from tasks.hierarchy import (DenormalizedHierarchy, Hierarchy, HierarchyChildParentsUnion, HierarchyInfoUnion,
                             HierarchyChildParent, GetParentsFunction, LevelHierarchy, LevelInfo)
from tasks.au.geo import (GEO_STE, GEO_SA4, GEO_SA3, GEO_SA2, GEO_SA1, GEO_MB, GEO_POA,
                          Geography)
from lib.logger import get_logger

LOGGER = get_logger(__name__)
COUNTRY = 'au'


LEVELS = [GEO_MB, GEO_SA1, GEO_SA2, GEO_SA3, GEO_SA4, GEO_POA, GEO_STE]


class AUDenormalizedHierarchy(DenormalizedHierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        return {
            'data': AUHierarchy(year=self.year),
            'function': AUGetParentsFunction(year=self.year),
            'rel': AUHierarchyChildParentsUnion(year=self.year, levels=LEVELS)
        }


class AUGetParentsFunction(GetParentsFunction):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        return {
            'data': AUHierarchy(year=self.year),
            'rel': AUHierarchyChildParentsUnion(year=self.year, levels=LEVELS),
            'info': AUHierarchyInfoUnion(year=self.year, levels=LEVELS),
        }


class AUHierarchy(Hierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        LOGGER.debug('Levels: {}'.format(LEVELS))
        return {
            'info': AUHierarchyInfoUnion(year=self.year, levels=LEVELS),
            'rel': AUHierarchyChildParentsUnion(year=self.year, levels=LEVELS)
        }


class AUHierarchyInfoUnion(HierarchyInfoUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        return [AULevelInfo(year=self.year, geography=level)
                for level in self.levels]


class AUHierarchyChildParentsUnion(HierarchyChildParentsUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        child_parents = self._child_parents()
        LOGGER.debug('Child-parents: {}'.format(child_parents))
        return {
            'hierarchy': [
                AUHierarchyChildParent(year=self.year,
                                       current_geography=child_parent[0],
                                       parent_geographies=child_parent[1])
                for child_parent in child_parents]
        }


class AUHierarchyChildParent(HierarchyChildParent):
    def requires(self):
        return {
            'level': AULevelHierarchy(year=self.year,
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


class AULevelHierarchy(LevelHierarchy):
    def requires(self):
        return {
            'current_info': AULevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': Geography(year=self.year,
                                      resolution=self.current_geography),
            'parents_infos': [AULevelInfo(year=self.year,
                                          geography=parent_geography) for
                              parent_geography in self.parent_geographies],
            'parents_geoms': [Geography(year=self.year,
                                        resolution=parent_geography) for
                              parent_geography in self.parent_geographies]
        }

    @property
    def _geoid_field(self):
        return 'geom_id'


class AULevelInfo(LevelInfo):
    def requires(self):
        return Geography(year=self.year, resolution=self.geography)

    @property
    def _geoid_field(self):
        return 'geom_id'

    @property
    def _geoname_field(self):
        return 'geom_name'
