import flow_writer.abstraction.pipeline as dfap
import flow_writer.abstraction.stage as dfas
from flow_writer.abstraction.dataflow import copy_dataflow
from flow_writer.abstraction.node import Node


def inject_after(dataflow, node, location=None):
    """
    Returns a new DFA with the new the addition of a new _node injected after 'name'
    Name is a 'Stage' or 'Step' name selector, also supporting a directory like form:
    eg 'STAGE A / STEP 1'
    """
    location = location.strip()

    if _classname(dataflow) == 'Pipeline':
        new_dataflow = _inject_in_pipeline_after(dataflow, node, location)
    else:
        new_dataflow = _inject_in_stage_after(dataflow, node, location)

    return copy_dataflow(new_dataflow, dataflow)


def _inject_in_stage_after(dataflow, node, location) -> (dfas.Stage, Node):
    """Injects a new _node in a newly created Stage after the _node 'location'"""
    node_index = dataflow.steps_map[location]
    steps = dataflow.steps[:node_index + 1] + [node] + dataflow.steps[node_index + 1:]
    stage = dfas.Stage(dataflow.name, *steps)
    return copy_dataflow(stage, dataflow)


def _inject_in_pipeline_after(dataflow, node, location) -> (dfap.Pipeline, Node):
    """Injects a new _node in a newly created Pipeline after the _node specified by 'location'"""
    stage, step = location.strip().split('/')
    stage = stage.strip()
    step = step.strip()

    stage_index = dataflow.stages_map[stage]

    stage_with_new_node = _inject_in_stage_after(dataflow.stages[stage_index], node, step)

    stages = dataflow.stages[:stage_index] + [stage_with_new_node] + dataflow.stages[stage_index + 1:]

    pipeline = dfap.Pipeline(dataflow.name, *stages)
    return pipeline


def inject_before(dataflow, node, location=None):
    """
    Returns a new DFA with the new the addition of a new _node injected before 'name'
    Name is a 'Stage' or 'Step' name selector, also supporting a directory like form:
    eg 'STAGE A / STEP 1'
    """
    location = location.strip()

    if _classname(dataflow) == 'Pipeline':
        new_dataflow = _inject_in_pipeline_before(dataflow, node, location)
    else:
        new_dataflow = _inject_in_stage_before(dataflow, node, location)

    return copy_dataflow(new_dataflow, dataflow)


def _inject_in_stage_before(dataflow, node, location) -> (dfas.Stage, Node):
    """Injects a new _node in before the _node specified by 'location' in a newly created Stage"""
    node_index = dataflow.steps_map[location]
    steps = dataflow.steps[:node_index] + [node] + dataflow.steps[node_index:]
    stage = dfas.Stage(dataflow.name, *steps)
    return copy_dataflow(stage, dataflow)


def _inject_in_pipeline_before(dataflow, node, location) -> (dfap.Pipeline, Node):
    """Injects a new _node in a newly created Pipeline before the _node specified by 'location'"""
    stage, step = location.strip().split('/')
    stage = stage.strip()
    step = step.strip()

    stage_index = dataflow.stages_map[stage]

    stage_with_new_node = _inject_in_stage_before(dataflow.stages[stage_index], node, step)

    stages = dataflow.stages[:stage_index] + [stage_with_new_node] + dataflow.stages[stage_index + 1:]

    pipeline = dfap.Pipeline(dataflow.name, *stages)
    return pipeline


def _classname(dataflow):
    return dataflow.__class__.__name__
