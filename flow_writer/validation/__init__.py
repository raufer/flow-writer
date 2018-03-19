import json
from pprint import pprint
from flow_writer.validation.exceptions import SignalNotValid


def validate_input_signal(validations, kwargs, fobj):
    """
    Confronts the input signal with a set of validations that need to respected in order for the validation test to pass
    Just useful for its side effects

    The signal needs to be extracted from the kwargs of the curried function.
    We expect the signal to be present in kwargs with the 'df' key
    'fobj' is the callable object in case we need to access meta fields

    In case of failure, a 'SignalNotValid' exception is thrown, breaking the execution of the pipeline
    This exception can be used to send errors invalid data through error channels
    """
    if 'df' not in kwargs:
        return None

    df = kwargs['df']

    if callable(validations):
        validations = [validations]

    signal_is_invalid = next(((f, i) for i, f in enumerate(validations) if not f(df)), False)

    if signal_is_invalid:
        val_name = signal_is_invalid[0].__name__
        val_pos = signal_is_invalid[1]
        step_name = fobj.__name__

        msg = {
            "message": "Signal failed the validation check",
            "failed validation": "'{}', nth validation '{}'".format(val_name, val_pos),
            "at node": "step '{}'".format(step_name)
        }

        print('\n' + json.dumps(msg, indent=4))

        raise SignalNotValid({
            "message": "Signal failed the validation check",
            "failed validation": "'{}', nth validation '{}'".format(val_name, val_pos),
            "at node": "step '{}'".format(step_name)
        })
