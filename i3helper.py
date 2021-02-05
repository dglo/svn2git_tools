#!/usr/bin/env python
"""
IceCube helper methods
"""

import os
import shutil
import sys
import tempfile

# Import either the Python2 or Python3 function to reraise a system exception
try:
    # pylint: disable=unused-import
    from reraise2 import reraise_excinfo  # pylint: disable=syntax-error
except SyntaxError:
    # pylint: disable=unused-import
    from reraise3 import reraise_excinfo


# Select the appropriate raw input function for Python2/Python3
if sys.version_info >= (3, 0):
    read_input = input      # pylint: disable=invalid-name
else:
    read_input = raw_input  # pylint: disable=invalid-name,undefined-variable

# Python3 redefined 'unicode' to be 'str'
if sys.version_info[0] >= 3:
    unicode = str


class Comparable(object):
    """
    A class can extend/mixin this class and implement the compare_key()
    method containing all values to compare in their order of importance,
    and this class will automatically populate the special comparison and
    hash functions
    """
    def __eq__(self, other):
        if other is None:
            return False
        return self.compare_key == other.compare_key

    def __ge__(self, other):
        return not self < other

    def __gt__(self, other):
        if other is None:
            return False
        return self.compare_key > other.compare_key

    def __hash__(self):
        return hash(self.compare_key)

    def __le__(self, other):
        return not self > other

    def __lt__(self, other):
        if other is None:
            return True
        return self.compare_key < other.compare_key

    def __ne__(self, other):
        return not self == other

    @property
    def compare_key(self):
        "Return the keys to be used by the Comparable methods"
        raise NotImplementedError(unicode(type(self)))


class TemporaryDirectory(object):
    """
    Context manager which runs inside a temporary directory.

    Example:
        with TemporaryDirectory() as tmpdir:
            ...do stuff in the temporary directory...
        ...temporary directory no longer exists..
    """

    def __init__(self):
        self.__origdir = os.getcwd()
        self.__scratchdir = None

    def __enter__(self):
        "Create and move to temporary directory"
        self.__scratchdir = tempfile.mkdtemp()
        os.chdir(self.__scratchdir)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        "Return to the original directory and remove the temporary directory"
        os.chdir(self.__origdir)
        shutil.rmtree(self.__scratchdir)
        return False

    @property
    def original(self):
        "Return the path to the original directory"
        return self.__origdir

    @property
    def name(self):
        "Return the path to the temporary directory"
        return self.__scratchdir
