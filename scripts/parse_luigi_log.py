#! /usr/bin/env python3


# Usage:
# a) Useful with `tail -f`: tail -f -n 1000 <path to luigi log> | ./parse_luigi_log.py
# b) Useful without -f, will display unfinished tasks: ./parse_luigi_log.py <path to luigi log>

import sys
import re
from datetime import datetime

if len(sys.argv) > 1:
    with open(sys.argv[1]) as f:
        log_lines = f.readlines()
    show_partial = True
else:
    log_lines = sys.stdin
    show_partial = False

tasks = {}

EXTRACT_RE = re.compile(r'(?P<date>\S*) (?P<time>\S*).*(?P<action>running|done|failed).*\btasks\.\b(?P<task>.*)')
TASK_SEPARATOR_RE = re.compile(r'===== Luigi Execution Summary =====')

try:
    for line in log_lines:
        match = EXTRACT_RE.match(line)
        if match:
            groups = match.groupdict()
            date = datetime.strptime(groups['date'] + ' ' + groups['time'],
                                     '%Y-%m-%d %H:%M:%S,%f')
            action = groups['action']
            task = groups['task']
            if task not in tasks.keys():
                tasks[task] = {}
            tasks[task][action] = date

            if tasks[task].get('running', None):
                if tasks[task].get('done', None) or tasks[task].get('failed',
                                                                    None):
                    action = 'done' if 'done' in tasks[task].keys() else 'failed'
                    elapsed = tasks[task][action] - tasks[task]['running']
                    print("{} {} {}".format(str(elapsed).split('.')[0], action,
                                            task))
        elif TASK_SEPARATOR_RE.match(line):
            print("\n\n===== New task\n\n")
    if show_partial:
        for task, task_info in tasks.items():
            if not task_info.get('done', None):
                for action, date in task_info.items():
                    elapsed = datetime.now() - date
                    print("{} {} {}".format(str(elapsed).split('.')[0], action, task))
except KeyboardInterrupt:
    sys.exit(1)
