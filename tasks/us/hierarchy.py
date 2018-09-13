from luigi import Task, Parameter, IntParameter
from tasks.us.census.tiger import SumLevel, SUMLEVELS
from lib.logger import get_logger

LOGGER = get_logger(__name__)


class USHierarchy(Task):
    year = IntParameter()

    def requires(self):
        return [
            USLevelHierarchy(year=self.year, current_geography=child_parent[0],
                             parent_geography=child_parent[1]) for child_parent
            in self._child_parents()]

    def _child_parents(self):
        child_parents = []
        previous = None
        for idx, level in enumerate(self._levels()):
            if previous:
                child_parents.append([level, previous])
            previous = level
        return child_parents

    def _levels(self):
        level_infos = [(level, info) for level, info in SUMLEVELS.items()]

        sorted_level_infos = sorted(level_infos,
                                    key=lambda level_info: level_info[1][
                                        'weight'])
        return [level_info[0] for level_info in sorted_level_infos]


class USLevelHierarchy(Task):
    year = IntParameter()
    current_geography = Parameter()
    parent_geography = Parameter()

    def requires(self):
        return {
            'current_sumlevel': SumLevel(year=self.year,
                                         geography=self.current_geography),
            'parent_sumlevel': SumLevel(year=self.year,
                                        geography=self.parent_geography),
        }

    def run(self):
        LOGGER.debug(
            "Generating hierarchy: {} < {}".format(self.current_geography,
                                                   self.parent_geography))
