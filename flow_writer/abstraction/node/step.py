from flow_writer.abstraction.node import Node


class Step(Node):
    """
    A processing element, a unit of computation, that has
    inputs and outputs ports to pass data.

    The computational work done by this unit is assumed to be functional.
    No side effects are allowed.
    """
    def __init__(self, f, location=None):
        super(Step, self).__init__(f, location)
        self.id = "Step [{}]".format(next(Node._id_gen))
