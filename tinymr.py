"""In-memory MapReduce."""


import abc
import builtins
from collections import defaultdict
import importlib.metadata
from inspect import isgeneratorfunction
import itertools as it
from functools import partial
import operator as op


__all__ = ["ElementCountError", "MapReduce"]


try:
    __version__ = importlib.metadata.version('tinymr')
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    # One benefit of 'tinymr' is that it is contained within a single file,
    # and can just be copied wherever. In these cases it is not actually
    # installed, and does not have package metadata. PEP-723 will be an
    # improvement: https://peps.python.org/pep-0723/
    __version__ = '0.0'


class MapReduce(abc.ABC):

    """In-memory MapReduce framework.

    Subclassers must implement ``mapper()`` and ``reducer()`` methods. Various
    other properties and methods control how a task is executed. Subclassers
    are free to implement an ``__init__()`` method to allow for configuration
    at instantiation.

    Once instantiated, callers must pass a stream of data to ``__call__()``.
    """

    @abc.abstractmethod
    def mapper(self, item):

        """Map phase. Subclassers must implement.

        Receives a single item from the input stream and produces one or more
        tuples.

        :param object item:
            A single item from the input data stream.

        :rtype tuple or generator:

        :return:
            One or more tuples in the form of: ``(key, value)`` or
            ``(key, sort, value)``. Can ``return`` a single tuple or ``yield``
            many. The presence of the ``sort`` element triggers sorting prior
            to calling ``reducer()``.
        """

        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def reducer(self, key, values):

        """Reduce phase. Subclassers must implement.

        Receives values corresponding to a single partition/key and produces
        one or more tuples.

        :param object key:
            The partition key. This is the first element from all the tuples
            emitted by ``mapper()``.
        :param list values:
            List of all values emitted by ``mapper``. May or may not be sorted
            depending on the ``mapper()`` implementation and the
            ``sort_map_with_value`` property. Sort order can be configured with
            the ``sort_map_reverse`` property.

        :rtype tuple or generator:

        :return:
            Like ``mapper()``, tuples take the form of ``(key, value)`` or
            ``(key, sort, value)``. Must ``return`` a single tuple or ``yield``
            many.

        Returns
        -------
        A ``tuple`` with 2 or 3 elements. Can also ``yield`` multiple
        ``tuple``s.
        """

        raise NotImplementedError  # pragma: no cover

    def output(self, mapping):

        """Optionally modify output data before it is returned to the caller.

        The contents of ``mapping`` depends on how ``reducer()`` is
        implemented. If ``reducer()`` yields multiple values then ``mapping``'s
        values will be a list of objects. If ``reducer()`` returns a single
        value ``mapping``'s keys will be a single object.

        :param dict mapping:
            A mapping between the first element produced by each ``reducer()``
            call and its corresponding values as a list.

        :rtype object:

        :return:
            Anything! The default implementation just passes on the input
            mapping unaltered, but subclassers are free to do whatever they
            wish in this method.
        """

        return mapping

    @property
    def sort_map_with_value(self):

        """Include the value key produced by ``mapper()`` when sorting.

        :rtype bool:
        """

        return False

    @property
    def sort_map_reverse(self):

        """Sort output of ``mapper()`` descending instead of ascending.

        :rtype bool:
        """

        return False

    @property
    def sort_reduce_with_value(self):

        """Like the ``sort_map_with_value`` property but for ``reducer()``.

        :rtype bool:
        """

        return False

    @property
    def sort_reduce_reverse(self):

        """Like the ``sort_map_reverse`` property but for ``reducer()``.

        :rtype bool:
        """

        return False

    def __call__(self, sequence, map=None, mapper_map=None, reducer_map=None):

        """Execute a MapReduce task.

        The map and/or reduce phases can be executed concurrently by passing a
        parallelized ``map()`` function to ``mapper_map`` and ``reducer_map``.
        For example, ``concurrent.futures.ProcessPoolExecutor.map()``.

        :param sequence sequence:
            Each item is passed to ``mapper()``.
        :param callable map:
            Default value for ``mapper_map`` and ``reducer_map``.
        :param callable mapper_map:
            An alternative implementation of the builtin ``map()`` function.
            Can be used to run the map phase across multiple processes or
            threads by passing something like ``multiprocessing.Pool.map()``.
        :param callable reducer_map:
            Like ``mapper_map`` but for the reduce phase.

        :rtype object:

        :return:
            See ``output()``.
        """

        # If 'mapper()' is a generator, and it will be executed in some job
        # pool, wrap it in a function that expands the returned generator
        # so that the pool can serialize results and send back. Be sure to
        # wrap properly to preserve any docstring present on the method.
        mapper = self.mapper
        if mapper_map is not None and isgeneratorfunction(self.mapper):
            mapper = partial(_wrap_mapper, mapper=self.mapper)

        # Same as 'mapper()' but for 'reducer()'.
        reducer = self.reducer
        if reducer_map is not None:
            reducer = partial(_wrap_reducer, reducer=self.reducer)

        # Run map phase. If 'mapper()' is a generator flatten everything to
        # a single sequence.
        mapper_map = mapper_map or builtins.map
        mapped = mapper_map(mapper, sequence)
        if isgeneratorfunction(self.mapper):
            mapped = it.chain.from_iterable(mapped)

        # Partition and sort (if necessary).
        partitioned = _partition_and_sort(
            mapped,
            sort_with_value=self.sort_map_with_value,
            reverse=self.sort_map_reverse)

        # Run reducer. Be sure not to hold on to a pointer to the partitioned
        # dictionary. Instead, replace it with a pointer to a generator.
        reducer_map = reducer_map or it.starmap
        partitioned = partitioned.items()
        reduced = reducer_map(reducer, partitioned)

        # If reducer is a generator expand to a single sequence.
        if isgeneratorfunction(self.reducer):
            reduced = it.chain.from_iterable(reduced)

        # Partition and sort (if necessary).
        partitioned = _partition_and_sort(
            reduced,
            sort_with_value=self.sort_reduce_with_value,
            reverse=self.sort_reduce_reverse)

        # The reducer can yield several values, or it can return a single
        # value. When the operating under the latter condition extract that
        # value and pass that on as the single output value.
        if not isgeneratorfunction(self.reducer):
            partitioned = {k: next(iter(v)) for k, v in partitioned.items()}

        # Be sure not to pass a 'defaultdict()' as output.
        return self.output(dict(partitioned))


def _wrap_mapper(item, mapper):

    """Use when running concurrently to normalize mapper output.

    Expands generator produced by ``MapReduce.mapper()`` so that results can
    be serialized and returned by a worker.

    :param object item:
        For ``MapReduce.mapper()``.
    :param callable mapper:
        A ``MapReduce.mapper()``.

    :rtype tuple:

    :return:
        Output of ``mapper`` expanded into a tuple.
    """

    return tuple(mapper(item))


def _wrap_reducer(key_values, reducer):

    """Like ``_wrap_mapper()`` but for ``MapReduce.reducer()``.

    :param tuple key_values:
        Arguments for ``MapReduce.reducer()``. First element is the key and
        second is values.
    :param callable reducer:
        A ``MapReduce.reducer()``.

    :rtype tuple:

    :return:
        Output of ``reducer`` wrapped in a tuple.
    """

    return tuple(reducer(*key_values))


class ElementCountError(Exception):

    """Raise when the actual element count does not match expectations."""


def _partition_and_sort(sequence, sort_with_value, reverse):

    """Partition and sort data after mapping but before reducing.

    Given the output from ``mapper()`` or ``reducer()``, partition,
    sort if necessary, remove any data that was only used for sorting.

    :param iterable sequence:
        Of tuples. Output from ``mapper()`` or ``reducer()``.
    :param bool sort_with_value:
        Indicates if data should be sorted based on the value element in
        addition to any sort elements that may be present.
    :param bool reverse:
        Indicates if data should be sorted descending instead of ascending.

    :rtype dict:

    :return:
        Where keys are partitions and values are ready to be passed to
        ``reduce()`` or ``output()``. All extra sorting information has
        been removed.
    """

    sequence = (s for s in sequence)
    first = next(sequence)
    sequence = it.chain([first], sequence)

    if len(first) not in (2, 3):
        raise ElementCountError(
            "Expected data of size 2 or 3, not {}. Example: {}".format(
                len(first), first))

    has_sort_element = len(first) == 3
    need_sort = has_sort_element or sort_with_value

    if has_sort_element:
        sequence = map(op.itemgetter(0, slice(1, 3)), sequence)

    if not need_sort:
        getval = None
        sortkey = None

    elif not has_sort_element and sort_with_value:
        def getval(x):
            return x
        sortkey = None

    else:
        getval = op.itemgetter(1)
        if sort_with_value:
            sortkey = None
        else:
            sortkey = op.itemgetter(0)

    partitioned = defaultdict(list)
    for ptn, vals in sequence:
        partitioned[ptn].append(vals)

    if need_sort:
        partitioned = {
            p: (
                v.sort(key=sortkey, reverse=reverse),
                list(map(getval, v))
            )[1]
            for p, v in partitioned.items()
        }

    return partitioned
