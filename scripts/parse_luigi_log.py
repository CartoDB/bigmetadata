#! /usr/bin/env python3


# Usage: tail -f -n 1000 <path to luigi log> | ./parse_luigi_log.py

import sys, re
from datetime import datetime

tasks = {}

EXTRACT_RE = r'(?P<date>\S*) (?P<time>\S*).*(?P<action>running|done|failed).*\btasks\.\b(?P<task>.*)'

for line in sys.stdin:
    match = re.compile(EXTRACT_RE).match(line)
    if match:
        groups = match.groupdict()
        date = datetime.strptime(groups['date'] + ' ' + groups['time'], '%Y-%m-%d %H:%M:%S,%f')
        action = groups['action']
        task = groups['task']
        if not task in tasks.keys():
            tasks[task] = {}
        tasks[task][action] = date

        if tasks[task].get('running', None):
            if tasks[task].get('done', None) or tasks[task].get('failed', None):
                action = 'done' if 'done' in tasks[task].keys() else 'failed'
                elapsed = tasks[task][action] - tasks[task]['running']
                print("{} {} {}".format(str(elapsed).split('.')[0], action, task))
