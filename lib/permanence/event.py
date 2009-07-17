# encoding: utf-8

"""
Makes it easy for objects to generate events and for listeners to respond to
those events.
"""

class EventSource(object):
    def __init__(self):
        super(EventSource, self).__init__()
        self.__event_listeners = {}
    
    def observe(self, event_name, listener):
        if event_name not in self.__event_listeners:
            self.__event_listeners[event_name] = []
        self.__event_listeners[event_name].append(listener)
    
    def fire(self, event_name, **kwargs):
        if event_name not in self.__event_listeners:
            return
        
        for listener in self.__event_listeners[event_name]:
            listener(**kwargs)
