import os
import json
from tasks.meta import OBSTimespan, current_session

TIMESPANS_FILE = 'timespans.json'
TIMESPAN_ID = 'id'
TIMESPAN_NAME = 'name'
TIMESPAN_DESCRIPTION = 'description'
TIMESPAN_TIMESPAN = 'timespan'


def get_timespan(timespan_id):
    JSONFile = os.path.join(os.path.dirname(__file__), TIMESPANS_FILE)
    with open(JSONFile) as infile:
        timespan_json = json.load(infile).get(str(timespan_id))

        timespan = OBSTimespan(id=timespan_json.get(TIMESPAN_ID),
                               name=timespan_json.get(TIMESPAN_NAME),
                               description=timespan_json.get(TIMESPAN_DESCRIPTION),
                               timespan=timespan_json.get(TIMESPAN_TIMESPAN))
        return current_session().merge(timespan)
