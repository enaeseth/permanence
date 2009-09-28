# encoding: utf-8

"""
The heart of the beast: brings all the parts together to make recordings.
"""

from __future__ import with_statement

from permanence.event import EventSource
from permanence.monitor import ProcessMonitor
import threading
import time
import contextlib
from Queue import Queue

class Recorder(EventSource):
    HOOKS = ("startup", "shutdown", "show_start", "show_error", "show_done",
        "show_save")
    
    def __init__(self, config):
        super(Recorder, self).__init__()
        self._hooks = self._create_hook_invoker(config.options)
        self.__reload_lock = threading.RLock()
        self.__config_updated = threading.Event()
        self._start_tasks = {}
        self._stop_tasks = {}
        
        self.apply_configuration(config)
    
    def _create_hook_invoker(self, options):
        invoker = HookInvoker(options.get("hook_pool_size", 2))
        for hook_name in self.HOOKS:
            invoker.create_hook(hook_name)
        invoker.observe_events(self)
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
                source = ("%s:%d" % (name, i + 1)
                    for i in enumerate(implementations))
            
            for description, impl in source:
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
        for (stop_time, stop_recording) in self._stop_tasks.itervalues():
            stop_recording()
        
    
    def _subprocesses_all_exited(self):
        ProcessMonitor.get_instance().halt()
        self.fire("shutdown")
    
    def _tick(self):
        if self.__config_updated.isSet():
            self._update_upcoming()
            self.__config_updated.clear()
        
        now = time.time()
        starts_to_clear = []
        for key, (start_time, start_recording) in self._start_tasks.iteritems():
            if now >= start_time:
                stop_task = start_recording()
                if stop_task:
                    self._stop_tasks[key] = stop_task
                starts_to_clear.append(key)
        
        for key in starts_to_clear:
            del self._start_tasks[key]
        
        stops_to_clear = []
        for key, (stop_time, stop_recording) in self._stop_tasks.iteritems():
            if now >= stop_time:
                stop_recording()
                stops_to_clear.append(key)
                source_name, show = key
                try:
                    source = self.sources[source_name]
                except KeyError:
                    # the source was removed since the recording session was
                    # started
                    continue
                
                self._set_start_task(source, show)
        
        for key in stops_to_clear:
            del self._stop_tasks[key]
    
    def _update_upcoming(self):
        found_keys = set()
        keys_to_remove = []
        for source_name, source in self.sources.iteritems():
            for show in source.shows:
                key = (source_name, show)
                
                if self._set_start_task(source, show):
                    found_keys.add(key)
        
        for key in self._start_tasks:
            if key not in found_keys:
                del self._start_tasks[key]
    
    def _set_start_task(self, source, show):
        start_task = self._create_start_task(source, show)
        if start_task:
            start_time = start_task[0]
            self.fire("show_schedule", source=source, show=show,
                start_time=start_time)
            key = (source.name, show)
            self._start_tasks[key] = start_task
            return key
        return None
    
    def _create_start_task(self, source, show):
        session = source.driver.spawn(show.name)
        
        start_time, duration = show.schedule.get_next_time()
        if not (start_time and duration):
            return None
        
        self._observe_session_events(source, show, session)
        
        def stop():
            try:
                session.stop()
            except RuntimeError:
                pass # there was nothing to stop
        
        def start():
            # We might not actually be starting at the scheduled start time;
            # for example, Permanence might have been started in the middle of
            # the show.
            now = time.time()
            real_duration = duration - (now - start_time)
            
            can_stop = session.can_stop_automatically(real_duration)
            
            stop_time = now + real_duration
            if can_stop:
                session.start(real_duration)
                # A stop action is created even if the session says it can stop
                # on its own. We want all sessions to be stopped gracefully
                # when the recorder is shut down, and since stop actions are
                # run on shutdown, creating one for self-stopping sessions is
                # an easy way to make that happen. Since ten seconds are added
                # to the stop time, the only other reason a session would be
                # stopped by the Recorder would be if the session was failing
                # to self-stop.
                stop_time += 10
            else:
                session.start()
            
            return (stop_time, stop)
        
        return (start_time, start)
    
    def _observe_session_events(self, source, show, session):
        def clear_stop_task():
            with self.__reload_lock:
                try:
                    del self._stop_tasks[(source.name, show)]
                except KeyError:
                    pass
        
        def started(session, **kwargs):
            self.fire("show_start", source=source, show=show)
        def error(session, error):
            clear_stop_task()
            self.fire("show_error", source=source, show=show, error=error)
        def finished(session, filename):
            clear_stop_task()
            self.fire("show_done", source=source, show=show, filename=filename)
            self._store_recording(source, show, filename)
        
        session.observe("start", started)
        session.observe("error", error)
        session.observe("done", finished)
    
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

