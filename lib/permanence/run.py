# encoding: utf-8

"""
The heart of the beast: brings all the parts together to make recordings.
"""

from __future__ import with_statement

import threading
import logging
import contextlib
from Queue import Queue

class HookInvoker(object):
    """
    Manages hook invocations in a pool of threads.
    """
    
    THREAD_NAME_PATTERN = "HookInvocationThread-%d"
    
    def __init__(self, pool_size, logger):
        self.__active = True
        self._logger = logger
        
        self._create_hook_dict()
        self._create_workers(pool_size)
    
    def create_hook(self, name):
        """
        Creates space for a hook with the given name.
        """
        with self._get_all_hooks() as hooks:
            if name in hooks:
                raise ValueError('a hook named %r has already been created' %
                    name)
            hooks[name] = []
    
    def register_hook(self, name, hook, description=None):
        """
        Causes the given hook to be called when hook events for the given
        name are generated.
        """
        with self._get_all_hooks() as hooks:
            try:
                hooks[name].append((hook, description))
            except KeyError:
                raise ValueError('no hook named %r has been registered' % name)
    
    def invoke(self, hook_name, **arguments):
        hooks = self._get_hooks(hook_name)
        if len(hooks) <= 0:
            return
        
        for hook, description in hooks:
            full_desc = "%s/%s" % (hook_name, description)
            self.__task_queue.put((hook, full_desc, arguments))
        
        with self.__task_available:
            if len(hooks) == 1:
                self.__task_available.notify()
            else:
                self.__task_available.notifyAll()
    
    def observe_events(self, event_source):
        """
        Listens for events on the given event source with the same name as
        the hooks defined in this invoker and translates them to hook
        invocations.
        """
        
        def valid_event_source():
            return (hasattr(event_source, 'observe') and
                hasattr(event_source.observe, '__call__')
        
        if not valid_event_source():
            raise TypeError("doesn't look like %r is a valid event source" %
                event_source)
        
        def map_event(name):
            def translate_event_to_hook(**arguments):
                self.invoke(name, **arguments)
        
        with self._get_all_hooks() as hooks:
            for name in hooks.iterkeys():
                map_event(name)
    
    def _create_hook_dict(self):
        # Do not access __hooks directly; see _get_all_hooks() and _get_hooks()
        self.__hooks = {}
        self.__hooks_lock = threading.RLock()
    
    def _create_workers(self, pool_size):
        self.__task_available = threading.Condition()
        self.__task_queue = Queue(0)
        self.__workers = []
        
        for i in xrange(pool_size):
            name = self.THREAD_NAME_PATTERN % (i + 1)
            thread = threading.Thread(target=self._run_hooks, name=name)
            self.__workers.append(thread)
            thread.start()
    
    @contextlib.contextmanager
    def _get_all_hooks(self):
        with self.__hooks_lock:
            yield self.__hooks
    
    def _get_hooks(self, hook_name):
        with self.__hooks_lock:
            try:
                return list(self.__hooks[hook_name])
            except KeyError:
                raise ValueError('no hook named %r has been registered' %
                    hook_name)
    
    def _run_hooks(self):
        while self.__active:
            with self.__task_available:
                while self.__task_queue.empty():
                    self.__task_available.wait()
                task = self.__task_queue.get()
            
            hook, description, arguments = task
            try:
                hook(**arguments)
            except Exception, e:
                self.logger.warn('hook %s failed: %s' % (description, e))

