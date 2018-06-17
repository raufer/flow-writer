import logging
import os

from flow_writer import config


def define_logging_file(logger, file=config["logs"]["pipeline"]["local"]):
    """
    Defines a logging file
    """
    try:
        # create local file if does not exist
        if not os.path.exists(file):
            basedir = os.path.dirname(file)
            if not os.path.exists(basedir):
                os.makedirs(basedir)
            with open(file, 'a'):
                os.utime(file, None)
        # define file handler
        fh = logging.FileHandler(file)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
    except:
        # TODO: should write to S3
        pass
    return logger
