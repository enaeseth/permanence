# encoding: utf-8

"""
Configuration loading and verification for Permanence.
"""

from __future__ import with_statement
from permanence.schedule import get_schedule
import yaml

class Configuration(object):
    """Configuration settings for Permanence."""
    def __init__(self, storage, sources, options):
        self.storage = storage
        self.sources = sources
        self.options = options
        
class RecordingSource(object):
    def __init__(self, name, driver, storage, shows):
        self.name = name
        self.driver = driver
        self.storage = storage
        self.shows = shows

class Show(object):
    def __init__(self, name, schedule):
        self.name = name
        self.schedule = schedule
        
def load_config(filename):
    def read_file():
        with open(filename, "rt") as config_file:
            return yaml.load(config_file)
    
    raw = read_file()
    for field in ('storage', 'sources'):
        if field not in raw:
            raise ConfigurationError('No %s are defined in the configuration '
                'file.')

    
class ConfigurationError(RuntimeError):
    pass
        
class NoSuchDriverError(RuntimeError):
    pass
    
def _get_driver_class(driver_type, driver_name):
    _globals, _locals = globals(), locals()
    def load_driver(module):
        imported = __import__(module, _globals, _locals, ['Driver'])
        try:
            return imported.Driver
        except AttributeError:
            raise ImportError('no "Driver" in %s' % module)
        
    try:
        driver = load_driver(driver_name)
    except ImportError:
        try:
            module = "permanence.%s.%s" % (driver_type, driver_name)
            driver = load_driver(module)
        except ImportError:
            raise NoSuchDriverError('no %s driver named "%s" could be found' %
                (driver_type, driver_name))
    
    return driver

def get_source_driver(driver_name, configuration):
    """
    Gets a new source driver object with the given name, set up with the given
    configuration.
    
    Raises a `NoSuchDriverError` if no source driver exists with the given
    name, and the source constructor may throw a `ConfigurationError` if there
    is anything wrong with its configuration.
    """
    
    driver_class = _get_driver_class("source", driver_name)
    return driver_class(configuration)
    
def get_storage_driver(driver_name, configuration):
    """
    Gets a new storage driver object with the given name, set up with the given
    configuration.
    
    Raises a `NoSuchDriverError` if no storage driver exists with the given
    name, and the storage constructor may throw a `ConfigurationError` if there
    is anything wrong with its configuration.
    """
    
    driver_class = _get_driver_class("storage", driver_name)
    return driver_class(configuration)
