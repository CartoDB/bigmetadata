import logging
import sys


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    return logging.getLogger(name)
