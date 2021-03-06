# encoding: utf-8

"""
Handling of show schedules.
"""

import re
import time
from permanence.config import ConfigurationError
from permanence.hook import add_json_serializer

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
    
    def get_next_time(self, leeway):
        now = time.localtime()
        
        start_time = self.start_time - leeway
        duration = self.duration + (leeway * 2)
        
        future = list(now)
        time_of_day = future[5] + (60 * future[4]) + (60 * 60 * future[3])
        weekday = now.tm_wday
        closest = self._get_closest_day_difference(weekday)
        if closest == 0 and time_of_day >= (start_time + duration):
            # the time today has already passed; move to the next occurring day
            weekday += 1
            closest = self._get_closest_day_difference(weekday) + 1
        
        future[3] = future[4] = future[5] = 0 # set time of day to midnight
        start = time.mktime(future) + (60 * 60 * 24) * closest
        
        start_time += start
        return (start_time, duration)
    
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
        
        def get_time(field):
            time = config.get(field)
            if time is None:
                return None
            
            if isinstance(time, basestring):
                match = re.match(r'^(?:(\d+):)?(\d+):(\d+)', time)
                if match:
                    parts = map(int, match.groups(0))
                    time = parts[2] + parts[1] * 60 + parts[0] * 60 * 60
                else:
                    raise ConfigurationError('invalid time value %r' % time)
            return time
                
        weekdays = get_days('weekdays') or get_days('weekday')
        if not weekdays:
            raise ConfigurationError('no weekdays defined in schedule')
        
        start_time = get_time('start')
        if start_time is None:
            raise ConfigurationError('no start time defined in schedule')
        
        end_time = get_time('end')
        if end_time is not None:
            if end_time < start_time:
                # show ends on following day; add 24 hours to end time
                end_time += (60 * 24 * 24)
            duration = end_time - start_time
        else:
            duration = get_time('duration')
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
    
    def json_friendly(self):
        return {
            "weekdays": self.weekdays,
            "start_time": self.start_time,
            "duration": self.duration
        }
_implementations['weekly'] = WeeklySchedule

add_json_serializer(WeeklySchedule, WeeklySchedule.json_friendly)
