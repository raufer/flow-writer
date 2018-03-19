from flow_writer.abstraction.dataflow import copy_dataflow
from flow_writer.builders import inject_after, inject_before
from flow_writer.abstraction.node.step import Step


class StepBuilder:
    """
    Helper builder to insert a new step into a pipeline.
    Returns a builder object with two locations selectors:
    - .after
    - .before

    >> pipeline.with_step(step).after('selector')
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
        side_effect = Step(self.f, location)
        dataflow = inject_after(self.container, side_effect, location)
        return self._construct(dataflow)

    def before(self, location=None):
        """
        Returns a new DFA with the new the addition of a new side effect injected before 'name'
        Name is a 'Stage' or 'Step' name selector, also supporting a directory like form:
        eg 'STAGE A / STEP 1'
        """
        side_effect = Step(self.f, location)
        dataflow = inject_before(self.container, side_effect, location)
        return self._construct(dataflow)

