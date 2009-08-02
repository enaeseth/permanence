# encoding: utf-8

"""
Manages temporary files.
"""

from tempfile import mkdtemp, mkstemp
import os

_state = dict(directory=None)

def get_temp_directory():
    if not _state["directory"]:
        _state["directory"] = mkdtemp(prefix="permanence_")
    return _state["directory"]

def open_temp_file(prefix=None, suffix=None):
    temp_dir = get_temp_directory()
    descriptor, path = mkstemp(prefix=prefix, suffix=suffix, dir=temp_dir)
    
    return (os.fdopen(descriptor), path) if descriptor else (None, None)

def create_temp_file(prefix=None, suffix=None):
    open_file, path = open_temp_file(prefix=prefix, suffix=suffix)
    if not open_file:
        return None
    else:
        open_file.close()
        return path
