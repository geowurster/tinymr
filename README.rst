tinymr
======

In-memory MapReduce.

``tinymr`` seeks to be:

#. Easy to use with minimal overhead.
#. Fast.
#. Tunable.
#. Occasionally actually useful.

Documentation
-------------

See `docs.rst <docs.rst>`_.

Documentation is built with `docutils <http://www.docutils.org>`_, which is
much lighter than Sphinx, but also has far fewer directives. It does support
rendering a single reStructuredText file as a single HTMl file though. The
project provides a helpful `cheatsheet <https://docutils.sourceforge.io/docs/user/rst/cheatsheet.txt>`_.

Developing
----------

.. code-block::

    # Set up workspace
    $ git clone https://github.com/geowurster/tinymr.git
    $ cd tinymr
    $ python3 -m venv venv
    (venv) $ pip install --upgrade pip setuptools
    (venv) $ pip install -r requirements-dev.txt -e ".[test]"

    # Run linters
    (venv) $ pycodestyle
    (venv) $ pydocstyle

    # Run tests
    (venv) $ pytest --cov tinymr --cov-report term-missing

    # Build docs
    (venv) $ docutils docs.rst docs.html

Last Working State
------------------

.. code-block::

    (venv) $ python3 --version
    Python 3.11.4

    (venv) $ python3 -c "import platform; print(platform.platform())"
    macOS-12.6.7-arm64-arm-64bit

    (venv) $ pip freeze
    coverage==7.3.2
    docutils==0.20.1
    iniconfig==2.0.0
    packaging==23.2
    pluggy==1.3.0
    pycodestyle==2.11.0
    pydocstyle==6.3.0
    Pygments==2.16.1
    pytest==7.4.2
    pytest-cov==4.1.0
    snowballstemmer==2.2.0

Noncommittal To Do List
-----------------------

* Move properties to descriptors.
* Attempt an async implementation.
* Use ``tox`` for testing.
* Use ``black``.
* Use ``mypy``.
