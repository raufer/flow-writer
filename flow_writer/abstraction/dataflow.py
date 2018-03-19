import logging
import copy
import itertools

from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class DataFlowAbstraction(ABC):
    """
    Abstraction for data abstraction execution
    This is the main trait to derive abstraction abstractions of different granularities
    """
    _id_gen = itertools.count()

    def __init__(self, name: str, *nodes):
        self.name = name.strip()
        self.side_effects = []
        self.description = None
        self.registry = {}
        self.profile = "default"


def copy_dataflow(dataflow_x: DataFlowAbstraction, dataflow_y: DataFlowAbstraction) -> DataFlowAbstraction:
    """
    The data flow_writer abstractions should be easy to use from the outside.
    It's very convenient to, at construct time, just pass a name a set of transformations
    DataFlow(name, step1, step2, ...)

    However, to ensure an immutable programming model we need to copy some attributes when we are creating derived flows
    """
    dataflow_x.description = str(dataflow_y.description) if dataflow_y.description else ''
    dataflow_x.side_effects = copy.deepcopy(dataflow_y.side_effects)
    dataflow_x.registry = copy.deepcopy(dataflow_y.registry)

    return dataflow_x
