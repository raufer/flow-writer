import collections
from itertools import chain, islice


def flatten(listOfLists):
    "Flatten one level of nesting"
    return chain.from_iterable(listOfLists)


def consume(iterator, n=None):
    """
    Advance the iterator n-steps ahead. If n is none, consume entirely.
    Use functions that consume iterators at C speed.
    """
    if n is None:
        # feed the entire iterator into a one length deque
        last = collections.deque(iterator, maxlen=1)
        return last.pop() if last else None
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)
