#!/usr/bin/env python

'''
Bigmetadata tasks

tasks to download and create metadata
'''

#import requests
#import datetime
#import json
#import csv
import json
import os
from luigi import Parameter, BooleanParameter, Task, WrapperTask, LocalTarget
from tasks.util import (LoadPostgresFromURL, classpath)
from psycopg2 import ProgrammingError



class LoadSocrataCSV(Task):

    domain = Parameter()
    dataset = Parameter()

    def requires(self):
        pass

    def target(self):
        pass
