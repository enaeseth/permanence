# encoding: utf-8

"""
Implements a storage driver that saves recordings as regular files.
"""

from permanence.config import ConfigurationError
from permanence.event import EventSource
from permanence.storage.util import compile_path_pattern
import os.path
import shutil

class FilesystemDriver(EventSource):
    def __init__(self, path_creator):
        super(FilesystemDriver, self).__init__()
        self.path_creator = path_creator
    
    def save(self, source, show, file_path):
        extension = os.path.splitext(file_path)[1]
        dest_path = self.path_creator(source, show) + extension
        directory, filename = os.path.split(dest_path)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        
        shutil.copy2(file_path, dest_path)
        self.fire("save", source=source, show=show, location=dest_path)
    
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
