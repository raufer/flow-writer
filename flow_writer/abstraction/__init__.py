import logging

from copy import deepcopy
from typing import Callable
from functools import wraps, partial
from pandas import DataFrame

from flow_writer.abstraction.dataflow import DataFlowAbstraction
from flow_writer.dependency_manager import dependency_manager
from flow_writer.ops.function_ops import cur, curr, inspect_parameters
from flow_writer.validation import validate_input_signal

logger = logging.getLogger(__name__)


def _extended_wraps(func, wrapper):
    """
    Our extended version of functools.wraps.
    Ensure that the result of the application of decorators is not order dependent
    """
    new_func = deepcopy(func)
    if hasattr(wrapper, "registry"):
        new_func.registry = wrapper.registry
    return new_func


def pipeline_step(val=None, dm=None, allow_rebinding=True) -> Callable:
    """
    Decorator to signal the fact that 'func' is to be used as a step in a data abstraction.
    It returns a curried version of it, which will be run when the curried version is passed
    a minimum number of arguments

    'val' can be a single predicate or a list of predicates of the form: (df: Signal -> Boolean)
    They should represent the constraints that a signal must respect in order to enter the node.

    'dm' should be a dict containing handles to manage possible step dependencies.
    This is in fact equivalent to applying the 'dependency_manager' decorator:

    By default rebinding of arguments is allowed, but can be controlled with the 'allow_rebind' flag

    usage:
    >>> @pipeline_step()
    >>> def step(df: Signal, *pars) -> Signal

    >>> def has_age_col(df):
    >>>		return 'age' in df.columns
    >>> @pipeline_step(val=has_age_col)
    >>> def step(df: Signal, *pars) -> Signal

    >>> dependencies = {"tokenizer": [loader(tokenizer_path), writer(tokenizer_path)]}
    >>>
    >>> @pipeline_step(dm=dependencies)
    >>> def step(df, tokenizer)
    """

    def decorator(func):

        func = _initialize_registry(func)

        if dm:
            func = dependency_manager(dm)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            new_func = _extended_wraps(func, wrapper)

            #  add hooks to be called at execution time
            #  one for input signal validations
            fvalidation = partial(validate_input_signal, val or [])
            new_func = _register_function(fvalidation, new_func, "input_validation")

            if allow_rebinding:
                return curr(new_func)(*args, **kwargs)
            else:
                return cur(new_func)(*args, **kwargs)

        return wrapper

    return decorator


def _call_with_requested_args(f, **kws):
    """Invoke 'f' using the arguments requested by it that should be available on 'kws'"""
    args_requested = inspect_parameters(f)
    fkws = {arg: kws[arg] for arg in args_requested if arg in kws}
    logger.info("Calling '{}' with '{}'".format(f.__name__, _pretty_logging(fkws)))
    logger.debug("Calling '{}' with '{}'".format(f.__name__, fkws))
    f(**fkws)


def _initialize_registry(f):
    """
    Initializes a "registry" on the callable "f".
    This serves as a registry of functions that will act as sink
    nodes that will run at specific moments of the execution workflow
    """
    if not hasattr(f, 'registry'):
        f.registry = {}
    return f


def _register_function(f, on, namespace):
    """
    Register a function 'f' on a callable object 'on' in the requested 'namespace'
    """
    on.registry[namespace] = f
    return on


def _pretty_logging(data):
    """
    Returns the dictionary 'data' with a suitable form for logging
    """
    def _picker(x):
        if isinstance(x, DataFlowAbstraction):
            return x.name

        elif isinstance(x, DataFrame):
            return "pd.DataFrame({})".format(str(x.columns))

        elif isinstance(x, tuple):
            return (_picker(i) for i in x)

        else:
            return x

    return {k: _picker(v) for k, v in data.items()}
