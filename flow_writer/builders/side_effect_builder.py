from flow_writer.abstraction.dataflow import copy_dataflow
from flow_writer.builders import inject_after, inject_before
from flow_writer.abstraction.node.side_effect import SideEffect


class SideEffectBuilder:
    """
    Handles the construction of the new dataflow unit
    Ensures all of the properties of the base unit are propagated to the newly derived one
    Additionally appends the new side effect

    We create a SideEffect object, that represents the managed side effect

    Helper object to create a 'managed' side effect.
    Returns a builder object with two locations selectors:
    - .after
    - .before

    The selector should follow a directory like structure, ie: 'stage a/step 1'
    """
    def __init__(self, container, f):
        self.f = f
        self.container = container

    def _construct(self, dataflow):
        """
        Handles the construction of the new dataflow unit
        Ensures all of the properties of the base unit are propagated to the newly derived one
        Additionally appends the new side effect

        We create a SideEffect object, that represents the managed side effect
        """
        dataflow = copy_dataflow(dataflow, self.container)
        return dataflow

    def after(self, location=None):
        """
        Returns a new DFA with the new the addition of a new side effect injected after 'name'
        Name is a 'Stage' or 'Step' name selector, also supporting a directory like form:
        eg 'STAGE A / STEP 1'

        We create a SideEffect object, that represents the managed side effect
        """
        side_effect = SideEffect(self.f, location)
        dataflow = inject_after(self.container, side_effect, location)
        return self._construct(dataflow)

    def before(self, location=None):
        """
        Returns a new DFA with the new the addition of a new side effect injected before 'name'
        Name is a 'Stage' or 'Step' name selector, also supporting a directory like form:
        eg 'STAGE A / STEP 1'
        """
        side_effect = SideEffect(self.f, location)
        dataflow = inject_before(self.container, side_effect, location)
        return self._construct(dataflow)

