# encoding: utf-8

"""
The heart of the beast: brings all the parts together to make recordings.
"""

from __future__ import with_statement

from permanence.event import EventSource
from permanence.monitor import ProcessMonitor
from permanence.hook import get_hook
import threading
import time
import contextlib
from Queue import Queue

class ShowManager(EventSource):
    class ManagedShow(object):
        def __init__(self, token, source, start_time, duration):
            self.token = token
            self.source = source
            self.start_time = start_time
            self.duration = duration
            self.session = None
            self.stop_time = None
    
    def __init__(self):
        super(ShowManager, self).__init__()
        self._shows = {}
        self._show_access = threading.RLock()
    
    def _get_next_time(self, schedule, leeway):
        return schedule.get_next_time(leeway) or (None, None)
    
    def add_show(self, key, token, source, schedule, leeway):
        with self._show_access:
            start_time, duration = self._get_next_time(schedule, leeway)
            
            if key not in self._shows:
                self._shows[key] = self.ManagedShow(token, source, start_time,
                    duration)
                self.fire('schedule', key=key, token=token,
                    start_time=start_time, duration=duration)
                return True
            
            existing = self._shows[key]
            same_time = (existing.duration == duration and
                existing.start_time == start_time)
            identical = (existing.source == source and same_time)
            if identical:
                return False
            
            if not same_time:
                self.fire('schedule', key=key, token=token,
                    start_time=start_time, duration=duration)
            
            existing.token = token
            existing.source = source
            existing.start_time = start_time
            existing.duration = duration
            
            if existing.session and existing.stop_time:
                # if the new schedule increases the stop time of the session,
                # update it
                
                new_stop_time = start_time + duration
                if existing.stop_time < new_stop_time:
                    existing.stop_time = new_stop_time
            
            return True
    
    def get_keys(self):
        with self._show_access:
            return set(self._shows.iterkeys())
    
    def remove_show(self, key):
        with self._show_access:
            if key not in self._shows:
                return None
            
            show = self._shows[key]
            if not show.session:
                # this show is not currently being recorded; just delete it
                del self._shows[key]
            else:
                # clear out the record; it will be removed when the recording
                # session is done
                show.source = show.start_time = show.duration = None
            return show.token
    
    def get_shows_to_start(self):
        now = time.time()
        
        with self._show_access:
            return [(key, s.token, s.source, s.duration - (now - s.start_time))
                for key, s in self._shows.iteritems()
                if s.session is None and s.source and now >= s.start_time]
    
    def set_session(self, key, session, stop_time):
        with self._show_access:
            try:
                show = self._shows[key]
            except KeyError:
                return False
            else:
                show.session = session
                show.stop_time = stop_time
                return True
    
    def get_sessions_to_stop(self):
        sessions = []
        now = time.time()
        
        with self._show_access:
            for key, show in self._shows.iteritems():
                if show.session is None or now < show.stop_time:
                    continue
                
                sessions.append((key, show.token, show.session))
            
            for session in sessions:
                del self._shows[session[0]]
        
        return sessions
    
    def get_all_sessions(self):
        with self._show_access:
            return [(key, show.session)
                for key, show in self._shows.iteritems()
                if show.session is not None]

class Recorder(EventSource):
    HOOKS = ("startup", "shutdown", "show_start", "show_error", "show_done",
        "show_schedule", "show_save")
    
    def __init__(self, config):
        super(Recorder, self).__init__()
        self._hooks = self._create_hook_invoker(config.options)
        self.__reload_lock = threading.RLock()
        self.__config_updated = threading.Event()
        self._manager = ShowManager()
        self._manager.observe('schedule', self._show_scheduled)
        
        self.apply_configuration(config)
    
    def _create_hook_invoker(self, options):
        invoker = HookInvoker(options.get("hook_pool_size", 2))
        for hook_name in self.HOOKS:
            invoker.create_hook(hook_name)
        invoker.observe_events(self)
        
        def hook_failed(description, error):
            self.fire("hook_failure", description=description, error=error)
        invoker.observe("failure", hook_failed)
        
        return invoker
    
    def apply_configuration(self, config):
        with self.__reload_lock:
            self._setup_hooks(config.hooks)
            
            self.storage = config.storage
            self.sources = config.sources
            self.options = config.options
            
            self._observe_storage_drivers()
            self.__config_updated.set()
    
    def _setup_hooks(self, hooks):
        """Registers the given hooks on this recorder."""
        invoker = self._hooks
        
        invoker.clear()
        for name, implementations in hooks.iteritems():
            try:
                source = implementations.iteritems()
            except AttributeError:
                source = (("%s:%d" % (name, i + 1), implementations[i])
                    for i in enumerate(implementations))
            
            for description, impl in source:
                impl = get_hook(impl, [])
                invoker.register_hook(name, impl, description)
    
    def _observe_storage_drivers(self):
        for driver in self.storage.itervalues():
            driver.observe("save", self._recording_saved)
            driver.observe("error", self._recording_error)
    
    def start(self):
        self.__shutdown_condition = threading.Condition()
        self.__active = True
        
        self._run()
    
    def stop(self):
        if not self.__active:
            raise RuntimeError("cannot stop; Recorder is not running")
        
        self.__active = False
        with self.__shutdown_condition:
            self.__shutdown_condition.notify()
        
        for driver in self.storage.itervalues():
            if hasattr(driver, 'shutdown'):
                driver.shutdown()
    
    def _run(self):
        self.fire("startup")
        with self.__shutdown_condition:
            while self.__active:
                check_interval = self.options.get("check_interval", 1.0)
                with self.__reload_lock:
                    self._tick()
                self.__shutdown_condition.wait(check_interval)
        self._shutdown()
        
    def _shutdown(self):
        # Shut down the hook invoker threads; they will finish any current
        # work and then terminate. (The process will not exit until the invoker
        # threads stop; they are not daemon threads.)
        self._hooks.stop()
        
        monitor = ProcessMonitor.get_instance()
        monitor.observe("empty", self._subprocesses_all_exited)
        
        # Stop any recording tasks that are in progress. Shutdown will continue
        # when all recording subprocesses exit.
        for key, session in self._manager.get_all_sessions():
            try:
                session.stop()
            except RuntimeError:
                pass
    
    def _subprocesses_all_exited(self):
        ProcessMonitor.get_instance().halt()
        self.fire("shutdown")
    
    def _tick(self):
        if self.__config_updated.isSet():
            self._update_manager()
            self.__config_updated.clear()
        
        now = time.time()
        for key, token, driver, duration in self._manager.get_shows_to_start():
            stop_time = now + duration
            
            source, show = token
            session = driver.spawn(key[1])
            can_stop = session.can_stop_automatically(duration)
            if can_stop:
                stop_time += 3
            
            self._observe_session_events(source, show, session)
            session.start(duration if can_stop else None)
            self._manager.set_session(key, session, stop_time)
            
        for key, token, session in self._manager.get_sessions_to_stop():
            try:
                session.stop()
            except RuntimeError:
                pass
            
            self._reschedule_show(*key)
    
    def _update_manager(self):
        existing_keys = self._manager.get_keys()
        updated_keys = set()
        
        for source_name, source in self.sources.iteritems():
            for show in source.shows:
                key = (source_name, show.name)
                updated_keys.add(key)
                token = (source, show)
                
                changed = self._manager.add_show(key, token, source.driver,
                    show.schedule, self.options.get('leeway', 0))
                if changed:
                    event = ('show_update' if key in existing_keys
                        else 'show_add')
                    self.fire(event, source=source, show=show)
        
        keys_to_remove = (existing_keys - updated_keys)
        for key in keys_to_remove:
            token = self._manager.remove_show(key)
            if token:
                self.fire('show_remove', source=token[0], show=token[1])
    
    def _observe_session_events(self, source, show, session):
        def started(session, **kwargs):
            self.fire("show_start", source=source, show=show)
        def error(session, error):
            self.fire("show_error", source=source, show=show, error=error)
        def finished(session, filename):
            self.fire("show_done", source=source, show=show, filename=filename)
            self._store_recording(source, show, filename)
        
        session.observe("start", started)
        session.observe("error", error)
        session.observe("done", finished)
    
    def _show_scheduled(self, key, token, start_time, duration):
        source, show = token
        self.fire("show_schedule", source=source, show=show,
            start_time=start_time)
    
    def _reschedule_show(self, source_name, show_name):
        try:
            source = self.sources[source_name]
        except KeyError:
            # that source has been removed
            return
        
        for show in source.shows:
            if show.name == show_name:
                key = (source.name, show.name)
                token = (source, show)
                
                self._manager.add_show(key, token, source.driver,
                    show.schedule, self.options.get('leeway', 0))
                return True
        
        return False
    
    def _store_recording(self, source, show, temp_file):
        for driver in source.storage:
            driver.save(source, show, temp_file)
        
    def _recording_saved(self, source, show, location):
        self.fire("show_save", source=source, show=show, location=location)
    
    def _recording_error(self, source, show, error):
        self.fire("show_error", source=source, show=show, error=error)

class HookInvoker(EventSource):
    """
    Manages hook invocations in a pool of threads.
    """
    
    THREAD_NAME_PATTERN = "HookInvocationThread-%d"
    
    def __init__(self, pool_size):
        super(HookInvoker, self).__init__()
        self.__active = True
        
        self._create_hook_dict()
        self._create_workers(pool_size)
    
    def stop(self):
        if self.__active:
            self.__active = False
            with self.__task_available:
                self.__task_available.notifyAll()
    
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
    
    def clear(self):
        with self._get_all_hooks() as hooks:
            for name in hooks.iterkeys():
                while len(hooks[name]) > 0:
                    hooks[name].pop()
    
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
                hasattr(event_source.observe, '__call__'))
        
        if not valid_event_source():
            raise TypeError("doesn't look like %r is a valid event source" %
                event_source)
        
        def map_event(name):
            def translate_event_to_hook(**arguments):
                self.invoke(name, **arguments)
            event_source.observe(name, translate_event_to_hook)
        
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
                    if not self.__active:
                        return
                    else:
                        self.__task_available.wait()
                task = self.__task_queue.get()
            
            hook, description, arguments = task
            try:
                hook(**arguments)
            except Exception, e:
                self.fire("failure", description=description, error=e)

