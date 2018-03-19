import logging
import yaml
import sys
import os

sys.setrecursionlimit(10000)

_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def quiet_py4j(level=logging.WARN):
    """
    Turn down spark verbosity for testing environmnets
    """
    logger = logging.getLogger('py4j')
    logger.setLevel(level)


def get_data(path):
    return os.path.abspath(os.path.join(_ROOT, path))


if hasattr(sys.stdout, 'isatty'):
    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

with open(os.path.join(get_data('flow_writer/properties/config.yaml'))) as config_file:
    config = yaml.load(config_file)

