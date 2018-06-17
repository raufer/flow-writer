import inspect
import types
from functools import wraps, partial

from flow_writer.ops.exceptions import PositionalRebindNotAllowed


def _call_original(func, args, kwargs):
    """
    check if original function has condition to be executed
    """
    func_args = inspect_parameters(func)
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


def genCur(func, execOnDemand=False):
    """
    Generates a 'curried' version of a function.

    We should be can bind arguments in any order we like and how many arguments we feel like at once.
    When currying a function, we should take care of possible destructive effects that the currying process might
    apply to the callable structure such as '__doc__' or '__name__'

    'functool.wraps' is being used to assure this integrity, so that the currying is done in a non-destructive way.

    'execOnDemand' is a flag indicating if the curried function execution should be deferred until an
    explicit request of invocation is made, i.e. by calling the function with no arguments 'f()'.
    In fact this increases the execution control by the caller.
    Just allows a rebinding operation by keyword argument.
    Since the execution of the function must be done explicitly, attempts to rebind arguments by position may raise an error
    """
    signature = signature_args(func)
    defaultargs = default_args(func)

    def _is_callable(elements):
        """Check we have available the minimum required arguments defined in'func'"""
        return sum(map(len, elements)) >= len(signature)

    @wraps(func)
    def f(*args, **kwargs):

        kwargs = {**defaultargs, **kwargs}

        if _is_callable([args, kwargs]) and not execOnDemand:
            return func(*args, **kwargs if len(args) < len(signature) else {})

        @wraps(func)
        def g(*callargs, **callkwargs):

            args_to_kwargs = {k: v for k, v in zip(_expected(func, kwargs), args + callargs)}

            newstate = {
                **kwargs,
                **args_to_kwargs,
                **callkwargs
            }

            if execOnDemand and _is_callable([args, kwargs]) and len(callargs) > 0:
                raise PositionalRebindNotAllowed

            if any([
                not execOnDemand and _is_callable([newstate]),
                execOnDemand and sum(map(len, [callargs, callkwargs])) == 0 and _is_callable([newstate])
            ]):
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


def curr(f):
    """Returns a curried version of 'f' and allows for argument rebinding"""
    return genCur(f)


def cur(f):
    """
    Returns a curried version of 'f' that is just invoked when an explicit request is made,
    i.e. by calling the function with no arguments 'f()'
    This increases the execution control by the caller.

    Notes:
    The curried version will not perform as expected when used out of context.
    Just allows a rebinding operation by keyword argument.
    Since the execution of the function must be done explicitly,
    attempts to rebind arguments by position should raise an error

    We call 'genCur(f)' and we execute it immediately genCur(f)().
    If we do not do this, the input domain of genCur does not include functions of the type:
    f :: Unit -> A
    Why is this? When we call the curried version for the first time it enters the underlying 'g' function,
    i.e. 'def g(*callargs, **callkwargs)' (check the source code)
    This one can handle the type mentioned above whereas the 'previous level', i.e.  'def f(*args, **kwargs)', does not.
    This is functionally similar to:
        type Special = Unit -> a
        typeOf(f) == Special ? genCur(f)() : genCur(f)
    """
    return genCur(f, execOnDemand=True)()


def inspect_parameters(callable):
    """
    Returns the named arguments of the callable objects
    usage:
    >> l = lambda name: name.lower()
    >> inspect_parameters(l) // ['name']
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
    f_args = inspect_parameters(f)

    defaultargs = default_args(f)

    closed_vars = get_closed_variables(f)

    args_closedvars = dict(zip(f_args, closed_vars.get('args', {})))

    kw_closedvars = closed_vars.get('kwargs', {})

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


def copy_func(f, name=None):
    """
    return a function with same code, globals, defaults, closure, and
    name (or provide a new name)

    Note: do not use copy.copy/deepcopy to clone a function since the
    callable object attributes are not properly handled
    """
    fn = types.FunctionType(
        f.__code__,
        f.__globals__,
        name or f.__name__,
        f.__defaults__,
        f.__closure__
    )
    fn.__dict__.update(f.__dict__)
    return fn
