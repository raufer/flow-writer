import types
import inspect

def is_lambda(obj):
    """
    Helper method to check if the 'obj' is a callable of the type 'Lambdatype'
    """
    return isinstance(obj, types.LambdaType) and obj.__name__ == "<lambda>"

def inspect_parameters(callable):
    """
    Returns the named arguments of the callable objects
    usage:
    >> l = lambda name: name.lower()
    >> inspect_parameters(l) // ['name']
    """
    params = inspect.signature(callable).parameters
    return list(params)
