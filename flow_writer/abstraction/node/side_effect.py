from flow_writer.abstraction.node import Node


class SideEffect(Node):
    """
    This class represents a 'managed' side effect in a data flow_writer.
    This computational unit is expected to mutate the world in some way.

    Being explicit about side effects enhances modularity and creates opportunity to have a better grasp over the whole pipeline behaviour
    """
    def __init__(self, f, location=None):
        super(SideEffect, self).__init__(f, location)
        self.id = "SideEffect [{}]".format(next(Node._id_gen))
