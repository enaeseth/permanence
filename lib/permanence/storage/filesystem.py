# encoding: utf-8

"""
Implements a storage driver that saves recordings as regular files.
"""

from permanence.config import ConfigurationError
from permanence.event import EventSource
from permanence.storage.util import compile_path_pattern
import os.path
import shutil
import time
import re

class FilesystemDriver(EventSource):
    def __init__(self, path_creator):
        super(FilesystemDriver, self).__init__()
        self.path_creator = path_creator
    
    def save(self, source, show, file_path):
        extension = os.path.splitext(file_path)[1]
        dest_filename = self.path_creator(source, show) + extension
        
        shutil.copy2(file_path, dest_filename)
        self.fire("save", source=source, show=show, location=dest_filename)
    
    @classmethod
    def from_config(cls, config):
        if not "location" in config:
            raise ConfigurationError("invalid filesystem storage driver "
                "config: no storage location provided")
        
        try:
            creator = compile_path_pattern(config["location"])
        except ValueError, e:
            raise ConfigurationError("invalid filesystem storage location: "
                "%s" % e)
        return cls(creator)
    
Driver = FilesystemDriver
