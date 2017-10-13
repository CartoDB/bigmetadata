import json
from collections import OrderedDict


CONDITIONS = "conditions"
EXCEPTIONS = "exceptions"


class ColumnsDeclarations:
    def __init__(self, JSONFile):
        with open(JSONFile) as file:
            self._columns = json.load(file)

    def _find_column(self, colid):
        return self._columns[colid]

    def _is_column_filtered(self, colid, parameters):
        params = json.loads(parameters)

        conditions = self._find_column(colid).get(CONDITIONS)
        if conditions is not None:
            if not self._check_requirements(params, conditions):
                return False

        exceptions = self._find_column(colid).get(EXCEPTIONS)
        if exceptions is not None:
            if self._check_requirements(params, exceptions):
                return False

        return True

    def _check_requirements(self, parameters, requirements):
        if not requirements:
            return True

        for requirement in requirements:
            check = True
            for id, value in parameters.iteritems():
                try:
                    if requirement[id] != value:
                        check = False
                except:
                    pass
            if check:
                return True

        return False

    def filter_columns(self, columns, parameters):
        filtered = OrderedDict()
        for colid, value in columns.iteritems():
            if self._is_column_filtered(colid, parameters):
                filtered[colid] = value
        return filtered
