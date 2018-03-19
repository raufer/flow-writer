from copy import deepcopy
from typing import Callable
from functools import wraps, partial

from flow_writer.dependency_manager import dependency_manager
from flow_writer.ops.function_ops import cur, curr, signature_args
from flow_writer.validation import validate_input_signal


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

        if dm:
            func = dependency_manager(dm)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            #  add hooks to be called at execution time
            #  one for input signal validations
            kwargs['__hook_input_validation'] = partial(validate_input_signal, val or [])
            new_func = _extended_wraps(func, wrapper)
            if allow_rebinding:
                return curr(new_func)(*args, **kwargs)
            else:
                return cur(new_func)(*args, **kwargs)

        return wrapper

    return decorator


def _call_with_requested_args(f, **kws):
    """Invoke 'f' using the arguments requested by it that should be available on 'kws'"""
    args_requested = signature_args(f)
    fkws = {arg: kws[arg] for arg in args_requested if arg in kws}
    f(**fkws)
