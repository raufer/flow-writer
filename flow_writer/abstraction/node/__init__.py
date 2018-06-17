import itertools

from flow_writer.utils.print import color_text
from abc import ABC, abstractmethod


class Node(ABC):
    """
    Abstract class defining an interface that supports all of the primitive operations needed for a generic dataflow node
    A “node” is a processing element that takes inputs, does some operation and returns the results on its outputs.
    It is a unit of computation. The actual computation doesn’t matter because dataflow is primarily concerned about moving data around.

    The simplest dataflow node has one input port and one output port
    It receives a computational unit 'f' that will perform the actual computation.
    The node never has the chance to look inside into this computation.
    Nodes are often functional, but it is not required.
    """

    _id_gen = itertools.count()

    def __init__(self, f, location=None):
        if not hasattr(f, "registry"):
            f.registry = dict()
        self.f = f
        self.location = location
        self.id = "Node [{}]".format(next(Node._id_gen))
        self.name = self._name_accessor(f)
        self.docs = _get_function_docs(f)

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

    def _name_accessor(self, f):
        """Access the name of the abstraction abstraction on which we are acting as container"""
        return getattr(f, '__name__')

    # @abstractmethod
    def initialize(self):
        """
        Raw graph initialization
        """
        pass

    @property
    def registry(self):
        return self.f.registry


def _get_function_docs(f):
    """Extract the docs of a step (callable) for documentation purposes"""
    # doc = re.sub(r"\.\n? +", r"\.\n? ", f.__doc__.strip())
    if not getattr(f, '__doc__'):
        return ""

    f_docs = f.__doc__
    doc = color_text(f_docs.strip().replace('  ', ' ').replace('\n  ', '\n\t'), 'white')

    return doc


def extract_signal(args):
    """Extracts signal from the returned values of each step"""
    return args[0] if isinstance(args, (tuple, list)) else args


def copy_node(node_x: Node, node_y: Node) -> Node:
    """
    Copies the attributes of 'node_y' to 'node_x'
    To ensure an immutable programming model we must ensure a node is properly copied
    """
    node_x.location = str(node_y.location)
    node_x.id = str(node_y.id)
    return node_x
