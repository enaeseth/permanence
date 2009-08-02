# encoding: utf-8

"""
Process monitoring.
"""

from permanence.event import EventSource
from collections import deque
import threading
import time

def monitor_process(open_process, callback):
    """
    Monitors a Popen object to see when its process terminates. When it does
    terminate, the given callback will be called with the process's exit code
    as its sole parameter.
    """
    
    ProcessMonitor.get_instance().monitor(open_process, callback)

class ProcessMonitor(EventSource):
    def __init__(self):
        super(ProcessMonitor, self).__init__()
        self.__processes = deque()
        self._thread = None
        
    @classmethod
    def get_instance(cls):
        if not hasattr(cls, "_global_instance"):
            cls._global_instance = cls()
        return cls._global_instance
    
    def monitor(self, process, callback):
        self.__processes.append((process, callback))
        
        if not self._thread:
            self.start()
            
    def start(self):
        if self._thread:
            raise RuntimeError("process monitor already started")
        
        self._thread = threading.Thread(target=self._check_processes,
            name="ProcessMonitorThread")
        self._active = True
        self._thread.start()
    
    def halt(self):
        self._active = False
        
    def _check_processes(self):
        while self._active:
            try:
                first_seen = None
                while len(self.__processes) > 0:
                    process, callback = self.__processes.popleft()
                    if not first_seen:
                        first_seen = process
                    elif process is first_seen:
                        self.__processes.append((process, callback))
                        break
                    
                    process.poll()
                    if process.returncode is not None:
                        callback(process.returncode)
                        if process is first_seen:
                            first_seen = None
                    else:
                        self.__processes.append((process, callback))
            except IndexError:
                pass
            
            if len(self.__processes) <= 0:
                self.fire("empty")
            time.sleep(0.25)
