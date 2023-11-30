``tinymr``
==========

In-memory `MapReduce <https://en.wikipedia.org/wiki/MapReduce>`_ framework.

.. contents:: Table of Contents
    :depth: 2

Brief MapReduce Overview
------------------------

MapReduce is a programming model for accomplishing distributed computing tasks
on very large datasets by essentially reading data in a distributed manner,
and then determining out which pieces of the dataset must be sent to the same
compute node for processing. The process looks like this:

#. Read data.
#. Apply a ``mapper`` function to each element of data. This function can
   produce zero, one, or many new data elements. Each is assigned a ``key``.
   Each ``key`` may or may not be unique.
#. All data elements are grouped together into ``partitions`` based on ``key``.
#. Data for each ``key`` is sorted.
#. All data for each ``key`` is passed to a single ``reducer`` function to
   produce a final value for that key.
#. Write data.

The Word Count Example
----------------------

An iconic and unavoidable example for demonstrating how MapReduce works is
counting words. A frequency distribution of the words in this paragraph, this
entire document, an entire book, or all books ever written is useful. The
license file for this repository (`LICENSE.txt <LICENSE.txt>`_) is accessible
and appropriate to test against.

Unfortunately, it is simpler if unicode is ignored and all text is assumed to
be ASCII. In reality, counting words in some languages is very difficult, but
the goal is only to demonstrate how MapReduce works, so this seems acceptable.

Another major problem is that text contains punctuation. The easiest way to
handle this is to replace all punctuation with a single space.

Pythonic Implementation
~~~~~~~~~~~~~~~~~~~~~~~

A simple implementation might be:

.. code:: python

    >>> from collections import Counter
    >>> counter = Counter()
    >>> with open('LICENSE.txt', encoding='ascii') as f:
    ...     for line in f:
    ...         for word in line.split():
    ...             word = word.lower()
    ...             counter[word] += 1
    >>> counter.most_common(3)
    [('the', 13), ('of', 12), ('or', 11)]

This is a perfectly appropriate and concise implementation that will work on
even a moderately sized text file, but this isn't as modularized and tunable
as MapReduce would like.

``map()`` and ``reduce()``
~~~~~~~~~~~~~~~~~~~~~~~~~~

MapReduce was inspired by the `map() <https://docs.python.org/3/library/functions.html#map>`_
and `reduce() <https://docs.python.org/3/library/functools.html#functools.reduce>`_
functions available in most programming languages. The `Pythonic Implementation`_
rewritten to employ ``map()`` and ``reduce()``:

.. code:: python

    >>> from collections import defaultdict
    >>> from functools import reduce
    >>> import itertools as it
    >>> import operator as op
    >>>
    >>> partitioned = defaultdict(list)
    >>> output = {}
    >>>
    >>> with open('LICENSE.txt', encoding='ascii') as f:
    ...
    ...     # Each element is all words in a single line.
    ...     words_by_line = map(op.methodcaller('split'), f)
    ...
    ...     # Flatten into a sequence of individual words.
    ...     words = it.chain.from_iterable(words_by_line)
    ...
    ...     # Mapping between words and a '1' for each instance of that word.
    ...     for word in words:
    ...         partitioned[word].append(1)
    ...
    ...     # Reduce one value per instance to a frequency count per word.
    ...     for word, instances in partitioned.items():
    ...         output[word] = reduce(op.add, instances)
    >>> Counter(output).most_common(3)
    [('OR', 8), ('OF', 8), ('the', 7)]

``mapper()`` and ``reducer()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using ``map()`` and ``reduce()`` directly leads to some awkward code, but
introducing ``mapper()`` and ``reducer()`` functions cleans things up a bit:

.. code:: python

    >>> from collections import defaultdict
    >>>
    >>> def mapper(line):
    ...     for word in line.split():
    ...         yield word, 1
    >>>
    >>> def reducer(word, occurrences):
    ...     return word, sum(occurrences)
    >>>
    >>> partitioned = defaultdict(list)
    >>> output = {}
    >>>
    >>> with open('LICENSE.txt') as f:
    ...     for line in f:
    ...         for word, count in mapper(line):
    ...             partitioned[word].append(count)
    >>>
    >>> for word, frequency in partitioned.items():
    ...     output[word] = sum(frequency)
    >>> Counter(output).most_common(3)
    [('OR', 8), ('OF', 8), ('the', 7)]

Word Count with ``tinymr``
~~~~~~~~~~~~~~~~~~~~~~~~~~

``tinymr`` offers a ``MapReduce()`` class that can be subclassed to implement
a MapReduce task. The same example using `mapper() and reducer()`_ can be
rewritten as:

.. code:: python

    >>> from tinymr import MapReduce
    >>>
    >>> class WordCount(MapReduce):
    ...
    ...     def mapper(self, line):
    ...         for word in line.split():
    ...             yield word, 1
    ...
    ...     def reducer(self, word, instances):
    ...         return word, sum(instances)
    ...
    ...     def output(self, mapping):
    ...         return Counter(mapping)
    >>>
    >>> wordcount = WordCount()
    >>>
    >>> with open('LICENSE.txt') as f:
    ...     counts = wordcount(f)
    >>>
    >>> counts.most_common(3)
    [('OR', 8), ('OF', 8), ('the', 7)]

``tinymr.MapReduce()`` hides all of the MapReduce stuff and only requires that
subclasses implement a few methods:

* *mapper()* - Receives one item from an input data stream and emits keys
  and values associated with that item. Use ``yield`` to emit many
  ``(key, value)`` tuples and ``return`` for a single pair. In this case,
  the input item is a line of text, and each ``(key, value)`` pair is a single
  ``word`` and a value of ``1`` indicating a single occurrence of that
  ``word``.
* *reducer()* - Receives one ``key`` from the ``mapper()`` and all of the
  associated ``values``. One ``word`` and a ``1`` for each instance of that
  ``word``. A new ``(key, value)`` pair is emitted by ``reducer()`` – or many
  if the implementation uses ``yield``. In this case ``key`` is a single
  ``word`` and ``value`` is the total number of instances of that word.
* *output()* - ``tinymr.MapReduce()`` provides a default implementation, but
  the example above overrides. This method is a mapping between

Note that ``tinymr.MapReduce()`` does not implement an ``__init__()`` method,
meaning that subclasses are given complete control over instantiation. This is
also a helpful place to store information that is needed in various method
calls.

Advanced Topics
---------------

Getting more out of ``tinymr.MapReduce()``!

Context Manager
~~~~~~~~~~~~~~~

Subclasses of ``tinymr.MapReduce()`` can implement the methods necessary to
turn the class into a `context manager <https://docs.python.org/3/library/stdtypes.html#typecontextmanager>`_.
This can be useful if your task needs to load data during instantiation and
cleared during teardown.

Overloading Keys
~~~~~~~~~~~~~~~~

Sometimes it is helpful to lean on MapReduce's execution model while abusing
how data is aggregated by key.

For example, the ``mapper()`` implemented in the `Word Count with tinymr`_
example operates on a single line of text. It treats each word as a key, and
assigns a value of ``1`` to each word. Each line of text could contain
multiple instances of a single word, and treating each instance independently
increases the memory requirement to complete the MapReduce task. This problem
is worse if ``mapper()`` operated on the contents of a single page of a book,
or even an entire book!

One solution would be to implement a ``mapper()`` like:

.. code:: python

    >>> def mapper(self, line):
    ...     counts = {}
    ...     for word in line.split():
    ...         if word not in counts:
    ...             counts[word] = 1
    ...         else:
    ...             counts[word] += 1

but another solution would be to leverage Python's ``collections.Counter()``.
Two instances of a ``Counter()`` can be added together:

.. code:: python

    >>> c1 = Counter(key=1)
    >>> c2 = Counter(key=3)
    >>> c1 + c2
    Counter({'key': 4})

One option is to do this:

.. code:: python

    >>> def mapper(self, line):
    ...     counts = Counter(line.split())
    ...     return counts.items()

but another option is:

.. code:: python

    >>> def mapper(self, line):
    ...     return None, Counter(line.split())

Using ``None`` as a key means that *all* of the ``Counter()`` instances are
routed to a single ``reducer()`` call. This makes more sense in the context
of a full ``tinymr.MapReduce()`` task:

.. code:: python

    >>> from collections import Counter
    >>> from functools import reduce
    >>> import operator as op
    >>>
    >>> from tinymr import MapReduce
    >>>
    >>> class WordCount(MapReduce):
    ...
    ...     def mapper(self, line):
    ...         return None, Counter(line.split())
    ...
    ...     def reducer(self, key, values):
    ...         return None, reduce(op.add, values)
    ...
    ...     def output(self, mapping):
    ...         return mapping[None]
    >>>
    >>> wordcount = WordCount()
    >>>
    >>> with open('LICENSE.txt') as f:
    ...     counts = wordcount(f)
    >>>
    >>> counts.most_common(3)
    [('OR', 8), ('OF', 8), ('the', 7)]

Note that in ``MapReduce.output()`` the input ``mapping`` is a
dictionary containing all keys, but in this case it contains a single ``None``
key with a single ``Counter()``. Returning this ``Counter()`` instance does
make this implementation of ``output()`` behave similarly to the parent method,
but ``output()`` can in fact do anything. It does not need to produce an
object that looks like a dictionary.

Sorting
~~~~~~~

The output of ``mapper()`` and ``reducer()`` can both optionally be sorted.
Both emit one or many tuples matching either ``(key, value)`` or
``(key, sort, value)``. The former is not sorted and the latter is sorted based
on the ``sort`` key. However, the former *can* be sorted by setting
``MapReduce.sort_map_with_value``, which causes the value key to be considered
when sorting. When the tuple also includes a ``sort`` element both the ``sort``
and ``value`` elements are considered when sorting. Descending sorting can be
enabled with ``MapReduce.sort_map_reverse``.

The table below demonstrates the interplay between different tuples and
properties. The same logic applies for the ``reducer`` variants.

======================  =======================  =================
Keys                    ``sort_map_with_value``  Sort Key
======================  =======================  =================
``(key, value)``        ``True``                 ``value``
``(key, value)``        ``False``                Sorting disabled
``(key, sort, value)``  ``True``                 ``(sort, value)``
``(key, sort, value)``  ``False``                ``sort``
======================  =======================  =================

Parallelism
~~~~~~~~~~~

MapReduce is designed for distributed compute environments, so it is only
natural that ``tinymr.MapReduce()`` also offer some mechanisms for parallel
execution. Typically a MapReduce application is deployed onto a compute cluster
and has lots of settings for configuring aspects of the process like the number
of nodes supporting each phase. Properly configuring a task is important
because it can impact how much data is moved between nodes, but ``tinymr``
operates in-memory and thus has a different set of considerations. Supporting
parallel execution inside of ``tinymr`` would also limit execution to whichever
distributed compute frameworks are directly implemented, or require some kind
of plugin architecture. Neither are much fun for what is supposed to be a
light, fast, and fun project.

``tinymr`` takes a different approach. Callers may pass a function with a
signature and behavior similar to Python's builtin ``map()`` function that
may run however and wherever it wants. The example below uses two different
thread pools for the map and reduce phases to perform a (inefficient) word
count across multiple files. Ideally this example would demonstrate using
processes as well, but this project uses ``doctest`` to ensure documentation
is correct, and it does not play nicely with with objects that must be pickled.
The pattern is the same.

.. code:: python

    >>> from concurrent.futures import ThreadPoolExecutor
    >>> from multiprocessing.dummy import Pool as ThreadPool
    >>> import os
    >>>
    >>> from tinymr import MapReduce
    >>>
    >>> class WordCount(MapReduce):
    ...
    ...     def mapper(self, path):
    ...         with open(path) as f:
    ...             for line in f:
    ...                 for word in line.split():
    ...                     yield word, 1
    ...
    ...     def reducer(self, key, values):
    ...         return key, sum(values)
    ...
    ...     def output(self, mapping):
    ...         return Counter(mapping)
    >>>
    >>> # Normally 'os.cpu_count()' would be used, but this code snippet is
    >>> # automatically tested and requires a stable value in all environments.
    >>> cpu_count = 2
    >>> infiles = ['LICENSE.txt'] * cpu_count
    >>>
    >>> threadpool1 = ThreadPool(cpu_count)
    >>> threadpool2 = ThreadPoolExecutor(cpu_count)
    >>>
    >>> wordcount = WordCount()
    >>> with threadpool1 as threadpool1, threadpool2 as threadpool2:
    ...     count = wordcount(
    ...         infiles,
    ...         mapper_map=threadpool1.map,
    ...         reducer_map=threadpool2.map
    ...     )
    >>> count.most_common(3)
    [('OR', 16), ('OF', 16), ('the', 14)]

In this case each ``reducer()`` is receiving a single word, so exeecuting each
``reducer()`` in a separate process/thread is very inefficient. Instead it may
be better to leverage how partitioning works by emitting a limited number of
keys in ``mapper()``. This example produces keys in a manner that ensures
``reduce()`` is called 4 times, can be replaced with the number of CPUS and
pairs well with passing ``multiprocessing.Pool()`` to ``reducer_map``. Its
output is a mapping between the four keys and the number of values passed to
each ``reducer()`` call.

.. code:: python

    >>> from collections import OrderedDict
    >>> import itertools as it
    >>>
    >>> from tinymr import MapReduce
    >>>
    >>> class KeyCount(MapReduce):
    ...
    ...     def mapper(self, line):
    ...         cycle = it.cycle(range(4))
    ...         for key, word in zip(cycle, line.split()):
    ...             yield key, word
    ...
    ...     def reducer(self, key, values):
    ...         return key, len(set(values))
    >>>
    >>> keycount = KeyCount()
    >>> with open('LICENSE.txt') as f:
    ...     count = keycount(f)
    >>> for key in sorted(count):  # Stability for doctest
    ...     print(key, count[key])
    0 52
    1 48
    2 49
    3 38
