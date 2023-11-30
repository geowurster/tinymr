import importlib.metadata

import tinymr


def test_version():

    """``tinymr.__version__`` comes from package metadata.

    This also indicates that the package is actually installed.
    """

    assert tinymr.__version__ == importlib.metadata.version('tinymr')
