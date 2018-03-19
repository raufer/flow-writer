from collections import namedtuple

from flow_writer.ops.function_ops import signature_args

DependencyEntry = namedtuple("DependencyEntry", ["sink", "source", "arg"])


def dependency_manager(dict_):
    """
    Decorator to handle the caching/persistence of certain arguments of a node.
    'f' and 'g' are two functions that have the responsibility of performing (load/save)-like operations.

    In the dataflow context, 'f' will be a source node, generating some data structure to be injected in the underlying function.
    'g' will be a sink node, receiving the return of the its associated step. The dataflow context will not capture any possible returns of this function.

    usage:
    >>> dependencies = {"tokenizer": [loader(tokenizer_path), writer(tokenizer_path)]}
    >>>
    >>> @dependency_manager(dependencies)
    >>> def step(df, tokenizer)
    """

    def decorator(func):
        dependency_entries = [DependencyEntry(source=value[0], sink=value[1], arg=key) for key, value in dict_.items()]

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
    """
    sink_args = signature_args(dependency_entry.sink)
    args = [step_output[registered_args.index(dependency_entry.arg)]]

    if 'df' in sink_args:
        df_index = sink_args.index('df')
        df_arg = step_output[registered_args.index('df')]
        args = ([df_arg] if df_index == 0 else []) + args + ([df_arg] if df_index == 1 else [])

    return args


def _step_dependencies_sinks(step, step_output):
    """Calls the functions (as side effects) registered for the 'step' that are managing its dependencies"""
    dependency_entries = step.registry.get("dependencies", list())
    registered_args = ['df'] + [c.arg for c in dependency_entries]
    for dependency_entry in dependency_entries:
        args = _extract_sink_arg(dependency_entry, registered_args, step_output)
        dependency_entry.sink(*args)
