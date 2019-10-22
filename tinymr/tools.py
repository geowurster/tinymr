"""Tools for working with data in a MapReduce context."""


import itertools as it

from tinymr import _compat


def slicer(iterable, chunksize):

    """
    Read an iterator in chunks.

    Example:

        >>> for p in slicer(range(5), 2):
        ...     print(p)
        (0, 1)
        (2, 3)
        (4,)

    Parameters
    ----------
    iterable : iter
        Input stream.
    chunksize : int
        Number of records to include in each chunk.  The last chunk will be
        incomplete unless the number of items in the stream is evenly
        divisible by `size`.

    Yields
    ------
    tuple
    """

    iterable = iter(iterable)
    while True:
        v = tuple(it.islice(iterable, chunksize))
        if v:
            yield v
        else:
            break


def mapkey(key, values):

    """
    Given a key and a series of values, create a series of `(key, value)`
    tuples.

    Example:

        >>> for pair in mapkey('key', range(5)):
        ...     print(pair)
        ('key', 0)
        ('key', 1)
        ('key', 2)
        ('key', 3)
        ('key', 4)

    Parameters
    ----------
    key : object
        Object to use as the first element of each output tuples.

    Returns
    -------
    iter
    """

    return _compat.zip(it.repeat(key), values)


def popitems(dictionary):

    """Like ``dict.popitem()`` but iterates over all ``(key, value)`` pairs,
    emptying the input ``dictionary``.  Useful for maintaining a lower memory
    footprint at the expense of some additional function calls.

    Parameters
    ----------
    dictionary : dict
        ``dict()`` to process.

    Yields
    ------
    tuple
        ``(key, value)``
    """

    while True:
        try:
            yield dictionary.popitem()
        except KeyError:
            break


def poplist(l):

    """Like ``list.pop(0)`` but iterates over all items in the input list and
    emptying it in the process.  Iterates from beginning to end.

    Parameters
    ----------
    l : list
        ``list()`` to process.

    Yields
    ------
    object
    """

    while True:
        try:
            yield l.pop(0)
        except IndexError:
            break


def single_key_output(items):

    """Override ``MapReduce.output()`` with a custom method that passes
    ``items`` to this method when dealing with outputs that only have a single
    value for every key.  For the standard word count example this would
    change the output from:

        (word1, (sum,)
        (word2, (sum,)
        (word3, (sum,)

    to:

        (word1, sum)
        (word2, sum)
        (word3, sum)

    The result is that the output can be passed directly to ``dict()``, if it
    fits in memory for more straightforwad key -> value lookups, rather than
    doing: ``next(iter(output[key]))``.

    Parameters
    ----------
    items : iter
        Stream of ``(key, values)`` pairs where ``values`` is also an iterable.

    Yields
    ------
    tuple
        The equivalent of ``(key, next(iter(values)))``.
    """

    for key, value in items:
        yield key, next(iter(value))
