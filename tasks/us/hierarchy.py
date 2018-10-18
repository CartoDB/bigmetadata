from luigi import WrapperTask
from tasks.hierarchy import (DenormalizedHierarchy, Hierarchy, HierarchyChildParentsUnion, HierarchyInfoUnion,
                             HierarchyChildParent, GetParentsFunction, LevelHierarchy, HierarchyChildParent, 
                             LevelInfo, _YearGeographyTask)
from tasks.base_tasks import TempTableTask
from tasks.meta import current_session
from tasks.us.census.tiger import GeoNamesTable, ShorelineClip, SUMLEVELS
from lib.logger import get_logger

LOGGER = get_logger(__name__)
COUNTRY = 'us'


def _levels():
    sorted_level_infos = reversed(sorted(SUMLEVELS.items(),
                                         key=lambda level_info:
                                         level_info[1]['weight']))
    return [level_info[0] for level_info in sorted_level_infos]


class USDenormalizedHierarchy(DenormalizedHierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        return {
            'data': USHierarchy(year=self.year),
            'function': USGetParentsFunction(year=self.year),
            'rel': USHierarchyChildParentsUnion(year=self.year, levels=levels)
        }


class USGetParentsFunction(GetParentsFunction):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        return {
            'data': USHierarchy(year=self.year),
            'rel': USHierarchyChildParentsUnion(year=self.year, levels=levels),
            'info': USHierarchyInfoUnion(year=self.year, levels=levels),
        }


class USHierarchy(Hierarchy):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        levels = _levels()
        LOGGER.info('Levels: {}'.format(levels))
        return {
            'info': USHierarchyInfoUnion(year=self.year, levels=levels),
            'rel': USHierarchyChildParentsUnion(year=self.year, levels=levels)
        }


class USHierarchyInfoUnion(HierarchyInfoUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        return [USLevelInfo(year=self.year, geography=level)
                for level in self.levels]


class USHierarchyChildParentsUnion(HierarchyChildParentsUnion):
    @property
    def _country(self):
        return COUNTRY

    def requires(self):
        child_parents = self._child_parents()
        LOGGER.info('Child-parents: {}'.format(child_parents))
        return {
            'hierarchy': [
                USHierarchyChildParent(year=self.year,
                                       current_geography=child_parent[0],
                                       parent_geographies=child_parent[1])
                for child_parent in child_parents]
        }


GEOGRAPHIES_ABBREVIATIONS = {
    'school_district_secondary': 'sds',
    'school_district_unified': 'sdu',
    'school_district_elementary': 'sde'
}


def abbr_tablename(target, geographies, year):
    abbrs = [GEOGRAPHIES_ABBREVIATIONS.get(geo, geo) for geo in geographies]

    if [x for x in zip(abbrs, geographies) if x[0] != x[1]]:
        splits = target.split('_')
        target = '_'.join([splits[0], '_'.join(abbrs), str(year), splits[-1]])

    return target


class USHierarchyChildParent(HierarchyChildParent):
    def requires(self):
        return {
            'level': USLevelHierarchy(year=self.year,
                                      current_geography=self.current_geography,
                                      parent_geographies=self.parent_geographies),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parent_geoms': [ShorelineClip(year=self.year,
                                           geography=parent_geography) for
                             parent_geography in self.parent_geographies]
        }


class USLevelHierarchy(LevelHierarchy):
    def requires(self):
        return {
            'current_info': USLevelInfo(year=self.year,
                                        geography=self.current_geography),
            'current_geom': ShorelineClip(year=self.year,
                                          geography=self.current_geography),
            'parents_infos': [USLevelInfo(year=self.year,
                                          geography=parent_geography) for
                              parent_geography in self.parent_geographies],
            'parents_geoms': [ShorelineClip(year=self.year,
                                            geography=parent_geography) for
                              parent_geography in self.parent_geographies]
        }

    def target_tablename(self):
        return abbr_tablename(super(USLevelHierarchy, self).target_tablename(),
                              [self.current_geography, 'parents'],
                              self.year)


class USLevelInfo(WrapperTask, _YearGeographyTask):
    GEOGRAPHIES_WITHOUT_GEONAMES = ['zcta5']

    def requires(self):
        if self.geography in self.GEOGRAPHIES_WITHOUT_GEONAMES:
            return USLevelInfoFromShorelineClip(year=self.year,
                                                geography=self.geography)
        else:
            return USLevelInfoFromGeoNames(year=self.year,
                                           geography=self.geography)

    def output(self):
        return self.input()


class USLevelInfoFromShorelineClip(LevelInfo):
    def requires(self):
        return ShorelineClip(year=self.year, geography=self.geography)

    @property
    def _geoid_field(self):
        return 'geoid'

    @property
    def _geoname_field(self):
        return 'geoid'

    def target_tablename(self):
        return abbr_tablename(super(USLevelInfoFromShorelineClip, self).target_tablename(),
                              [self.geography], self.year)


class USLevelInfoFromGeoNames(LevelInfo):
    def requires(self):
        return GeoNamesTable(year=self.year, geography=self.geography)

    @property
    def _geoid_field(self):
        return 'geoidsc'

    @property
    def _geoname_field(self):
        return 'geoname'

    def target_tablename(self):
        return abbr_tablename(super(USLevelInfoFromGeoNames, self).target_tablename(),
                              [self.geography], self.year)
