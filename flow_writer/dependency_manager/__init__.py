from collections import namedtuple

from flow_writer.dependency_manager.utils import unfold_steps_dependencies
from flow_writer.ops.function_ops import inspect_parameters, cur, curr, closed_bindings

DependencyEntry = namedtuple("DependencyEntry", ["sink", "source", "arg"])


def dependency_manager(dict_, apply_currying=True):
    """
    Decorator to handle the caching/persistence of certain arguments of a node.
    'f' and 'g' are two functions that have the responsibility of performing (load/save)-like operations.

    In the dataflow context, 'f' will be a source node, generating some data structure to be injected in the underlying function.
    'g' will be a sink node, receiving the return of the its associated step. The dataflow context will not capture any possible returns of this function.

    A sink definition is just useful for performing side effects with the result of a computation.
    Nevertheless there may be use cases where you actually do not need to receive anything
    from the underlying dataflow, so we create, for the writer, a curried version of it where the
    execution must be explicit. By doing so, we are able to bind the argument list in its whole and still defer
    the execution to a later time.
    Note that this is in contrast with a source node, that needs all its arguments so that we can retrieve its value
    and provide the computation with its external sources.

    The motivation behind 'apply_currying' argument is the following:

    1. Dependency functions are internally curried when added to a Step;
    2. Since the pipeline Steps are not mutable, new Steps are created after
        locking the dependencies for the functions in 1.;
    3. The process of creating new Steps uses the already implemented logic
        from point 1. but since the functions are already curried, we do not
        want to curry them again (since this brings issues of nested currying!)

    Thus, 'apply_currying' is a flag indicating if the underlying currying
    mechanism should be used to lift the functions. By doing so, this function
    can be used to update the dependencies handlers internally, i.e. already lifted

    usage:
    >>> dependencies = {"tokenizer": [loader, writer]}
    >>>
    >>> @dependency_manager(dependencies)
    >>> def step(df, tokenizer)
    """

    def id(x):
        return x

    ops = [curr if apply_currying else id, cur if apply_currying else id]

    def decorator(func):
        dependency_entries = [
            DependencyEntry(source=ops[0](value[0]), sink=ops[1](value[1]), arg=key)
            for key, value in dict_.items()
        ]

        func.registry = {
            'dependencies': dependency_entries
        }

        return func

    return decorator


def _extract_sink_arg(dependency_entry, registered_args, step_output):
    """
    Extracts the correspondent positional argument needed to run the sink in the dependency entry
    The entry should declare which argument it is managing.
    Once we have the name of it, we expected the user to return it following the same positional order.

    Additionally if the sink asks for the data signal, it will be also passed

    The signal can also be requested by a sink node. Assuming the set of arguments that is requested by the
    sink function is '[args]', 'df' can be requested to the left or to the right:
    (df, *[args]) | (*[args], df)
    """
    sink_args = inspect_parameters(dependency_entry.sink)
    args = [step_output[registered_args.index(dependency_entry.arg)]]

    if 'df' in sink_args:
        df_index = sink_args.index('df')
        head_pos = max([
            sink_args.index(arg) for arg in closed_bindings(dependency_entry.sink)
        ])
        df_arg = step_output[registered_args.index('df')]
        args = ([df_arg] if df_index == head_pos + 1 else []) + args + ([df_arg] if df_index == len(sink_args) - 1 else [])

    return args


def _step_dependencies_sinks(step, step_output):
    """Calls the functions (as side effects) registered for the 'step' that are managing its dependencies"""
    dependency_entries = step.registry.get("dependencies", list())
    registered_args = ['df'] + [c.arg for c in dependency_entries]
    for dependency_entry in dependency_entries:
        args = _extract_sink_arg(dependency_entry, registered_args, step_output)
        dependency_entry.sink(*args)()


