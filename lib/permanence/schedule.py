# encoding: utf-8

"""
Handling of show schedules.
"""

import re
import time
from permanence.config import ConfigurationError

_implementations = {}
def get_schedule(kind, definition):
    implementation = _implementations.get(kind)
    if not implementation:
        raise LookupError("unknown schedule type %r" % kind)
    
    return implementation.from_config(definition)

class WeeklySchedule(object):
    def __init__(self, weekdays, start_time, duration):
        self.weekdays = tuple(weekdays)
        self.start_time = start_time
        self.duration = duration
    
    def get_next_time(self):
        now = time.localtime()
        closest = self._get_closest_day_difference(now.tm_wday)
        
        future = list(now)
        future[3] = future[4] = future[5] = 0 # set time of day to midnight
        start = time.mktime(future) + (60 * 60 * 24) * closest
        
        start_time = start + self.start_time
        return (start_time, self.duration)
    
    def _get_closest_day_difference(self, today):
        return min((day - today) % 7 for day in self.weekdays)
    
    @classmethod
    def from_config(cls, config):
        def get_days(field):
            try:
                weekdays = config[field]
                if not isinstance(weekdays, list):
                    weekdays = re.split(r'\s*,\s*', weekdays)
                return [cls._convert_weekday(day) for day in weekdays]
            except KeyError:
                return None
                
        weekdays = get_days('weekdays') or get_days('weekday')
        if not weekdays:
            raise ConfigurationError('no weekdays defined in schedule')
        
        start_time = config.get('start')
        if start_time is None:
            raise ConfigurationError('no start time defined in schedule')
        
        end_time = config.get('end')
        if end_time is not None:
            if end_time < start_time:
                # show ends on following day; add 24 hours to end time
                end_time += (60 * 24 * 24)
            duration = end_time - start_time
        else:
            duration = config.get('duration')
            if not duration:
                raise ConfigurationError('no duration or end time defined '
                    'in schedule')
        
        return cls(weekdays, start_time, duration)
    
    _weekdays = {
        r'^M': 0,
        r'^Tu': 1,
        r'^W': 2,
        r'^Th': 3,
        r'^Fr': 4,
        r'^Sa': 5,
        r'^Su': 6
    }
    @classmethod
    def _convert_weekday(cls, day_name):
        for pattern, number in cls._weekdays.iteritems():
            if re.match(pattern, day_name):
                return number
        raise ConfigurationError('Invalid day of the week %r.' % day_name)
    
    @classmethod
    def _get_current_weekday(cls):
        return time.localtime().tm_wday
    
    def __repr__(self):
        return '%s(%r, %r, %r)' % (type(self).__name__, self.weekdays,
            self.start_time, self.duration)
    
    def __eq__(self, other):
        return (isinstance(other, WeeklySchedule) and
            other.weekdays == self.weekdays and
            other.start_time == self.start_time and
            other.duration == self.duration)
    
    def __hash__(self):
        return hash(("WeeklySchedule", self.weekdays, self.start_time,
            self.duration))
_implementations['weekly'] = WeeklySchedule
