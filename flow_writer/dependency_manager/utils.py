import logging

from operator import itemgetter
from operator import itemgetter as itg

from flow_writer.abstraction.node import copy_node
from flow_writer.abstraction.node.step import Step
from flow_writer.utils.recipes import merge_dicts, group_by
from flow_writer.ops.function_ops import signature_args, copy_func
from flow_writer.utils.recipes import explode, compose, flatten

logger = logging.getLogger(__name__)


def unfold_step_dependencies(step, include_source=True, include_sink=True):
    """
    Helper function to concatenate in a single list all of the expanded dependencies of each 'step' in 'steps'
    'include_source'/'include_sink' can be used to select the desired entries
    """
    dependencies = step.registry.get("dependencies", list())

    unfolded = explode(
        explode([(step, dependencies)], itemgetter(1)),
        lambda x: ([itemgetter(1)(x).source] if include_source else []) + (
            [itemgetter(1)(x).sink] if include_sink else []),
        lambda x: (itemgetter(0)(x), itemgetter(1)(x).arg)
    )

    unfolded = ((step, step_arg, f) for ((step, step_arg), f) in unfolded)

    unfolded = explode(
        unfolded,
        compose(signature_args, itemgetter(2)),
        lambda x: (itemgetter(0)(x), itemgetter(1)(x), itemgetter(2)(x))
    )

    unfolded = ((step, step_arg, f, f_arg) for ((step, step_arg, f), f_arg) in unfolded)

    return list(unfolded)


def unfold_steps_dependencies(steps, include_source=True, include_sink=True):
    """
    Unfold each step dependency with one granular entry per line.
    """
    return list(flatten([unfold_step_dependencies(s, include_source, include_sink) for s in steps]))


def generate_rebind_dict(stage, data, include_source=True, include_sink=True):
    """
    Handles the locking of the source of each dependency.
    It is expected each source function to have its argument
    list fully resolved since we need its value before executing the flow
    """
    _makepath = lambda val: "/".join([stage.name, val[0].name, val[1], val[2].__name__, val[3]])

    steps_with_dependencies = unfold_steps_dependencies(stage.steps, include_source, include_sink)
    steps_with_dependencies = (s for s in steps_with_dependencies if _makepath(s) in data)

    def _dependency_args(values):
        path = [_makepath(v) for v in values]
        return {p.split("/")[-1]: data[p] for p in path}

    grouped = group_by(lambda x: (itg(0)(x), itg(1)(x), itg(2)(x)), steps_with_dependencies)

    rebind_dict = merge_dicts([
        {step.name: {step_arg: f(**_dependency_args(values))}}
        for (step, step_arg, f), values in grouped.items()
    ])

    return rebind_dict


def update_dependencies(d1, d2, loc):
    """
    Returns a new dependencies dictionary by updating 'd1' with the new values of 'd2'
    'loc' sets the scope of the update: 'source' | 'sink'
    """
    for k, v in d1.items():
        if k in d2 and loc == 'sink':
            d2[k] = [copy_func(d1[k][0])] + [d2[k]]
        elif k in d2 and loc == 'source':
            d2[k] = [d2[k]] + [copy_func(d1[k][1])]
    d3 = d1.copy()
    d3.update(d2)
    return d3


def update_step_sink_dependencies(step, update):
    """
    Update sink dependencies of 'step'
    'update' should be a dictionary of the form: arg -> f
    """

    #  avoid circular imports
    from flow_writer.dependency_manager import dependency_manager

    h = copy_func(step.f)

    old_dependencies = {d.arg: [d.source, d.sink] for d in h.registry['dependencies']}
    dependencies = update_dependencies(old_dependencies, update, 'sink')

    new_step = copy_node(
        Step(dependency_manager(dependencies, apply_currying=False)(h)),
        step
    )

    return new_step


