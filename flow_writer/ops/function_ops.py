import inspect
import types
from functools import wraps, partial


def _call_original(func, args, kwargs):
    """
    check if original function has condition to be executed
    """
    func_args = signature_args(func)
    func_default_args = default_args(func)
    return len(set(func_args) - set(func_default_args) - set(kwargs)) <= len(args)


def _expected(f, kwargs):
    """
    Given a function 'f' and a dictionary with bound arguments,
    returns an ordered list of the arguments still needing a value

    >>> def f(a, b, c, d): return _
    >>> expected(f, {'a': 1, 'c': 2}) #  ['c', 'd]
    """
    return [a for a in signature_args(f) if a not in kwargs]


def _call_hooks(kwargs, func):
    """
    Call every registered hook, that will be probably just useful for its side effects
    We assume that every call to be made as the following key format: format '__hook_*'

    At the end we need to remove all of the hooks to ensure they are not used in the original function call
    """
    pattern = '__hook_'
    hooks = [v for k, v in kwargs.items() if pattern in k]

    for h in hooks:
        h(kwargs, func)

    return {k: v for k, v in kwargs.items() if pattern not in k}


def curry(func):
    signature = signature_args(func)
    defaultargs = default_args(func)

    @wraps(func)
    def f(*args, **state):

        state = {**defaultargs, **state}

        if sum(map(len, [args, state])) >= len(signature):
            return func(*args, **state if len(args) < len(signature) else {})

        @wraps(func)
        def g(*callargs, **callkwargs):

            args_to_kwargs = {k: v for k, v in zip(_expected(func, state), args + callargs)}

            newstate = {
                **state,
                **args_to_kwargs,
                **callkwargs
            }

            if len(newstate) >= len(signature):
                return func(**newstate)

            return f(**newstate)

        return g

    return f


def signature_args(callable):
    """
    Returns the named arguments of the callable objects
    usage:
    >> l = lambda name: name.lower()
    >> signature_args(l) // ['name']
    """
    params = inspect.signature(callable).parameters
    return list(params)


def get_closed_variables(callable):
    """
    This function inspects the closure of 'callable'
    It maps the free variables of the function to the values that are 'closed-over' in the function object

    Note: consider the use of 'inspect.getclosurevars(g)'
    """
    if not callable.__closure__:
        return dict()

    closedvars = dict(zip(
        callable.__code__.co_freevars,
        (c.cell_contents for c in callable.__closure__))
    )
    return closedvars


def is_lambda(obj):
    """
    Helper method to check if the 'obj' is a callable of the type 'Lambdatype'
    """
    return isinstance(obj, types.LambdaType) and obj.__name__ == "<lambda>"


def default_args(func):
    """
    Returns a dictionary of arg_name:default_values for the input function
    """
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


def closed_bindings(f):
    """
    Return all of the current variables bound to the function's closure.
    Note that the default behaviour does not include default arguments set for functions that were subsequently wrapped

    We are however extracting it from the original function. Obviously giving precedence to more recent bindings of default arguments
    """
    f_args = signature_args(f)

    defaultargs = default_args(f)

    closed_vars = get_closed_variables(f)

    args_closedvars = dict(zip(f_args, closed_vars.get('myArgs', {})))

    kw_closedvars = closed_vars.get('myKwArgs', {})

    return {**defaultargs, **args_closedvars, **kw_closedvars}


def lazy(func):
    """
    Decorator to defer the evaluation of 'func', returning a partial application of function with the arguments of its first call
    >>> @lazy
    ... def f(a, b, c): return a+b+c
    >>> r = f(1,2,3)
    >>> r() # 6
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return partial(func, *args, **kwargs)

    return wrapper
