# encoding: utf-8

"""
Utility code for storage drivers.
"""

import re
import sys
import time
import threading

class ActionQueue(object):
    def __init__(self, handler, worker_count=2, error_handler=None):
        self._handler = handler
        self._queue = []
        self._queue_control = threading.Lock()
        self._running = True
        self._error_handler = error_handler
        self._create_workers(worker_count)
    
    def _create_workers(self, worker_count):
        def create_thread():
            thread = threading.Thread(target=self._run)
            thread.start()
            return thread
        
        self._workers = [create_thread() for i in xrange(worker_count)]
    
    def _run(self):
        def execute_task(item, attempt):
            try:
                self._handler(item)
            except Exception:
                if self._error_handler:
                    self._error_handler(*sys.exc_info())
                self._schedule(item, attempt + 1)
        
        while self._running:
            with self._queue_control:
                now = time.time()
                
                for i in xrange(len(self._queue)):
                    if now >= self._queue[i][2]:
                        task = self._queue.pop(i)
                        execute_task(task[0], task[1])
                        break
            
            time.sleep(0.5)
    
    def add(self, item):
        self._schedule(item, 0)
    
    def _schedule(self, item, attempt):
        delay = (1.6 ** attempt) if attempt > 0 else 0
        
        with self._queue_control:
            self._queue.append((item, attempt, time.time() + delay))
    
    def shutdown(self):
        self._running = False

def compile_path_pattern(pattern):
    def path_formatter(fn):
        def path_format(source, show):
            return re.sub(r'__+', '_', re.sub(r'\W+', '', re.sub(r'\s+', '_',
                fn(source, show)))).lower()
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
            if hasattr(segment, '__call__'):
                value = segment(source, show)
            else:
                value = segment
            
            result.append(value)
        
        return ''.join(result)
    
    return fill_pattern
