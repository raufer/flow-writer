from functools import reduce
from typing import NoReturn
from operator import itemgetter as itg

from flow_writer.dependency_manager import unfold_steps_dependencies
from flow_writer.dependency_manager.exceptions import SourceArgumentsNotResolved, SinkArgumentsNotResolved
from flow_writer.ops.function_ops import default_args
from flow_writer.utils.recipes import group_by


def _makepath(stage, val):
    return "/".join([stage.name, val[0].name, val[1], val[2].__name__, val[3]])


def validate_required_arguments(stage, data, val_source_args=True, val_sink_args=True) -> NoReturn:
    """
    Raises an exception if 'data' does not provide every argument required by the dependency handlers
    """

    missing_source = _validate_source_dependencies(stage, data)

    if missing_source and val_source_args:
        raise SourceArgumentsNotResolved({
            "message": "Required source arguments not resolved",
            "missing arguments": missing_source
        })

    missing_sink = _validate_sink_dependencies(stage, data)

    if missing_sink and val_sink_args:
        raise SinkArgumentsNotResolved({
            "message": "Required sink arguments not resolved",
            "missing arguments": missing_sink
        })


def _validate_source_dependencies(stage, data):
    """
    Validate if all source dependencies required by 'stage' are provided by 'data'
    Returns the ones that are missing
    """
    steps_with_dependencies = unfold_steps_dependencies(stage.steps, include_sink=False)
    steps_with_dependencies = _remove_default_arguments(steps_with_dependencies)

    paths = [_makepath(stage, s) for s in steps_with_dependencies]
    return set(paths) - set(data)


def _validate_sink_dependencies(stage, data):
    """
    Validate if all sink dependencies required by 'stage' are provided by 'data'
    Returns the ones that are missing

    Resolving the required dependencies for a sink node is less trivial than doing it
    for the the source node. This is due to the fact that a sink node is expected to
    just be partially resolved, as opposed to a source node that should be totally resolved

    #  TODO: In general, any kind of validation of a sink node might too much of an assumption
    """
    steps_with_dependencies = unfold_steps_dependencies(stage.steps, include_source=False)
    steps_with_dependencies = _remove_default_arguments(steps_with_dependencies)

    paths = [_makepath(stage, s) for s in steps_with_dependencies]

    grouped = group_by(lambda x: (itg(0)(x), itg(1)(x), itg(2)(x), itg(3)(x)), [p.split("/") for p in paths])
    grouped = _remove_last_group_element(grouped)

    paths = reduce(
        lambda acc, group: acc + ["/".join(x) for x in group[1]],
        grouped.items(),
        []
    )

    return set(paths) - set(data)


def _remove_last_group_element(grouped):
    """
    Removes the last element of each group.
    Accommodates for the 'df' argument that might be required by a sink handler
    """
    def f(l):
        return l[:-1]

    result = {
        k: f([l for l in v if l[-1] != "df"])
        for k, v in grouped.items()
    }

    return result


def _remove_default_arguments(steps):
    """
    Removes default arguments.
    """
    steps = [
        step for step in steps
        if step[3] not in default_args(step[2])
    ]
    return steps
