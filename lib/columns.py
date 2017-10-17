import json
from collections import OrderedDict


CONDITIONS = "conditions"
EXCEPTIONS = "exceptions"


class ColumnsDeclarations:
    def __init__(self, JSONFile):
        with open(JSONFile) as file:
            self._columns = json.load(file)

    def _find_column(self, colid):
        return self._columns.get(colid)

    def _is_column_filtered(self, colid, parameters):
        params = json.loads(parameters)

        column = self._find_column(colid)
        if column:
            conditions = column.get(CONDITIONS)
            if conditions and not self._check_requirements(params, conditions):
                return False

            exceptions = column.get(EXCEPTIONS)
            if exceptions and self._check_requirements(params, exceptions):
                return False

        return True

    def _check_requirements(self, parameters, requirements):
        for requirement in requirements:
            for param_id, value in parameters.items():
                if param_id in requirement and requirement[param_id] != value:
                    break
            else:
                return True

        return False

    def filter_columns(self, columns, parameters):
        return OrderedDict([[k, v] for k, v in columns.items() if self._is_column_filtered(k, parameters)])
