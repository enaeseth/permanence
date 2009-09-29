# encoding: utf-8

"""
Implements a source driver that uses StreamRipper.
"""

from permanence.config import ConfigurationError
from permanence.event import EventSource
from permanence.monitor import monitor_process
from permanence.temp import get_temp_directory
from glob import glob
import subprocess
import time
import sys
import os
import os.path
import signal
import re

class StreamRipperDriver(object):
    def __init__(self, executable, stream):
        self.executable = executable
        self.stream = stream
        
    def spawn(self, show_name, identifier=None):
        return StreamRipperSession(self, show_name, identifier)
        
    @classmethod
    def from_config(cls, config):
        try:
            stream = config["stream"]
            executable = config.get("path", "streamripper")
        except KeyError:
            raise ConfigurationError("must provide the stream to rip using "
                "StreamRipper")
        return cls(executable, stream)
    
    def __repr__(self):
        return "%s(%r, %r)" % (type(self).__name__, self.executable,
            self.stream)

class StreamRipperSession(EventSource):
    def __init__(self, driver, show_name, identifier):
        super(StreamRipperSession, self).__init__()
        self.driver = driver
        self.show_name = show_name
        self.identifier = identifier
        self._ended = True
    
    def can_stop_automatically(self, duration):
        return (duration < sys.maxint) if hasattr(sys, "maxint") else True
        
    def _get_output_path(self):
        directory = get_temp_directory()
        name = re.sub(r'\W+', '', re.sub(r'\s+', '_', self.show_name)).lower()
        return os.path.join(directory, name)
    
    def start(self, duration=None):
        args = [self.driver.executable, self.driver.stream, "-A"]
        if duration:
            args.extend(["-l", "%d" % duration])
            self.duration = duration
            self.expected_shutdown = time.time() + duration - 5
        else:
            self.expected_shutdown = self.duration = None
        self.output_path = self._get_output_path()
        args.extend(["-a", self.output_path])
        
        self.start_time = time.time()
        black_hole = os.devnull or "/dev/null"
        self.stdin = open(black_hole, "r")
        self.stdout = open(black_hole, "w")
        
        try:
            self._process = subprocess.Popen(args, stdin=self.stdin,
                stdout=self.stdout, stderr=subprocess.STDOUT)
            self._ended = False
            self.fire("start", session=self, process=self._process,
                duration=duration)
            monitor_process(self._process, self._proc_ended)
        except Exception, e:
            self.fire("error", session=self,
                error="failed to start recording: %s" % e)
    
    def stop(self):
        if self._ended:
            raise RuntimeError("cannot stop StreamRipper process; process "
                "not running")
        try:
            self._process.terminate()
        except AttributeError:
            os.kill(self._process.pid, signal.SIGTERM)
    
    def _proc_ended(self, return_code):
        self._ended = True
        self._close_streams()
        
        now = time.time()
        elapsed_time = time.strftime("%Hh%Mm%Ss",
            time.gmtime(now - self.start_time))
        
        if return_code != 0:
            self.fire("error", session=self, error="streamripper exited with "
                "status %d after %s" % (return_code, elapsed_time))
        elif self.expected_shutdown and time.time() < self.expected_shutdown:
            expected_shutdown = time.strftime("%Hh%Mm%Ss",
                time.gmtime(self.duration))
            self.fire("error", session=self, error="streamripper exited "
                "early, after only %s (expected %s)" %
                (elapsed_time, expected_shutdown))
        else:
            filename = self._get_recorded_file_name()
            if filename:
                self.fire("done", session=self, filename=filename)
        
    def _close_streams(self):
        try:
            self.stdin.close()
            self.stdout.close()
        except Exception:
            pass
    
    def _get_recorded_file_name(self):
        pattern = "%s.*" % self.output_path
        matches = glob(pattern)
        if len(matches) == 0:
            self.fire("error", session=self, error="could not find "
                "streamripper output file (looked for %s)" % pattern)
        return matches[0]

Driver = StreamRipperDriver
