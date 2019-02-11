import logging
import sys

LUIGI_LOG_ARG = '--log-level'


def get_logger(name):
    '''
    Obtain a logger outputing to stderr with specified name. Defaults to INFO
    log level.
    '''
    logger = logging.getLogger(name)
    if LUIGI_LOG_ARG in sys.argv:
        index = sys.argv.index(LUIGI_LOG_ARG) + 1
        if len(sys.argv) > index:
            logger.setLevel(sys.argv[index])

    return logger
