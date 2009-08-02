# encoding: utf-8

"""
Implements a storage driver that saves recordings as regular files.
"""

from permanence.config import ConfigurationError
from permanence.event import EventSource
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
        
        shutil.copy2(source, dest_filename)
        self.fire("save", source=source, show=show, location=dest_filename)
    
    @classmethod
    def from_config(cls, config):
        if not "location" in config:
            raise ConfigurationError("invalid filesystem storage driver "
                "config: no storage location provided")
        
        try:
            creator = cls.compile_path_pattern(config["location"])
        except ValueError, e:
            raise ConfigurationError("invalid filesystem storage location: "
                "%s" % e)
        return cls(creator)
    
    @classmethod
    def compile_path_pattern(cls, pattern):
        def path_formatter(fn):
            def path_format(source, show):
                return re.sub(r'\W+', '', re.sub(r'\s+', '_', fn()))
            return path_format
        
        def get_segments():
            pos = 0
            segments = []
            
            while pos < len(pattern):
                next = pattern.find("{", pos)
                if next < 0:
                    segments.append(pattern[pos:])
                    break
                else:
                    segments.append(pattern[pos:next])
                
                end = pattern.find("}", next)
                if end < 0:
                    raise ValueError("missing '}' after '{' at pos %d" % next)
                
                spot = pattern[next + 1:end]
                pos = end + 1
                parts = spot.split("|")
                name, filters = parts[0], parts[1:]
                
                source = None
                if name == "source":
                    source = lambda source, show: source.name
                elif name == "show":
                    source = lambda source, show: show.name
                elif name == "date":
                    source = lambda source, show: time.strftime("%Y-%m-%d")
                else:
                    raise ValueError("invalid path variable %r at %d" %
                        (name, next + 1))
                
                for filter_name in filters:
                    if filter_name == "path_format":
                        source = path_formatter(source)
                    else:
                        raise ValueError("unknown formatter %r at %d" %
                            (filter_name, next + 1))
                
                segments.append(source)
                
            return segments
        
        segments = get_segments()
        
        def fill_pattern(source, show):
            result = []
            
            for segment in segments:
                value = None
                try:
                    value = segment(source, show)
                except TypeError:
                    value = segment
                
                result.append(value)
            
            return result
        
        return fill_pattern
Driver = FilesystemDriver
