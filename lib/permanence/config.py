# encoding: utf-8

"""
Configuration loading and verification for Permanence.
"""

# Note that much of the configuration code is error checking. The goal is to
# never accept a configuration that could cause Permanence to crash while
# running.

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
                'file.' % field)
        elif not isinstance(raw[field], dict):
            raise ConfigurationError('The %s field is not a mapping.' % field)
        
    storage = {}
    for key, definition in raw['storage'].iteritems():
        storage_type = definition.get('type')
        if not storage_type:
            raise ConfigurationError('The type of storage location %r is not '
                'defined.' % key)
        storage[key] = get_storage_driver(definition['type'], definition)
    
    sources = {}
    for source_name, definition in raw['sources'].iteritems():
        for field in ('storage', 'driver', 'shows'):
            if not definition.get(field):
                raise ConfigurationError('Source %r has no %s defined.' %
                    (source_name, field))
        
        if not isinstance(definition['driver'], dict):
            raise ConfigurationError('The driver field of source %r is not a '
                'mapping.' % source_name)
        elif not definition['driver'].get('type'):
            raise ConfigurationError('The type of the driver for source %r is '
                'not defined.' % source_name)
        
        driver_def= definition['driver']
        driver = get_source_driver(driver_def['type'], driver_def)
        
        source_storage = []
        try:
            for storage_key in definition['storage']:
                if storage_key not in storage:
                    raise ConfigurationError('Source %r: there is no storage '
                        'location named %r.' % (source_name, storage_key))
                source_storage.append(storage[storage_key])
        except TypeError:
            raise ConfigurationError('Storage for source %r must be given as '
                'a list of storage location names.' % source_name)
        
        if not isinstance(definition['shows'], dict):
            raise ConfigurationError('The shows of source %r must be given '
                'as a name -> details mapping.' % source_name)
        
        shows = []
        for show_name, show in definition['shows']:
            if not isinstance(show, dict):
                raise ConfigurationError('The definition of show %r is not a '
                    'mapping.' % show_name)
            elif not show.get('schedule'):
                raise ConfigurationError('Show %r does not define which kind '
                    'of schedule it uses.' % show_name)
            schedule = get_schedule(show['schedule'], show)
            
            shows.append(show_name, schedule)
        
        sources[source_name] = RecordingSource(source_name, driver,
            source_storage, shows)
    
    options = object()
    raw_options = raw.get('options')
    if raw_options:
        options.leeway = raw_options.get('leeway', 0)
    
    return Configuration(storage, sources, options)
    
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
    name, and the source driver may throw a `ConfigurationError` if there is
    anything wrong with its configuration.
    """
    
    driver_class = _get_driver_class("source", driver_name)
    return driver_class.from_config(configuration)
    
def get_storage_driver(driver_name, configuration):
    """
    Gets a new storage driver object with the given name, set up with the given
    configuration.
    
    Raises a `NoSuchDriverError` if no storage driver exists with the given
    name, and the storage driver may throw a `ConfigurationError` if there is
    anything wrong with its configuration.
    """
    
    driver_class = _get_driver_class("storage", driver_name)
    return driver_class.from_config(configuration)
