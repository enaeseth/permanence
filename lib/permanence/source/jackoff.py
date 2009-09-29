# encoding: utf-8

"""
Implements a source driver that uses Jackoff.
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

class JackoffDriver(object):
    def __init__(self, executable, ports, format, bitrate, channels, name):
        self.executable = executable
        self.ports = ports
        self.format = format
        self.bitrate = bitrate
        self.channels = channels
        self.client_name = name
    
    def spawn(self, show_name, identifier=None):
        return JackoffSession(self, show_name, identifier)
    
    @classmethod
    def from_config(cls, config):
        executable = config.get("path", "jackoff")
        name = config.get("name")
        format = config.get("format")
        bitrate = config.get("bitrate")
        channels = config.get("channels")
        ports = config.get("ports")
        
        if bitrate:
            bitrate = int(bitrate)
        if channels:
            channels = int(channels)
        
        return cls(executable, ports, format, bitrate, channels, name)
    
    def __repr__(self):
        return "%s(%r, %r, %r, %r, %r)" % (type(self).__name__,
            self.executable, self.format, self.bitrate, self.channels,
            self.name)

class JackoffSession(EventSource):
    def __init__(self, driver, show_name, identifier):
        super(JackoffSession, self).__init__()
        self.driver = driver
        self.show_name = show_name
        self.identifier = identifier
        self._ended = True
    
    def can_stop_automatically(self, duration):
        return (duration < sys.maxint) if hasattr(sys, "maxint") else True
    
    def _get_output_path(self):
        directory = get_temp_directory()
        name = re.sub(r'\W+', '', re.sub(r'\s+', '_', self.show_name)).lower()
        if not self.driver.format:
            name += ".aiff"
        else:
            name += "." + re.sub(r'\d+$', '', self.driver.format)
        return os.path.join(directory, name)
    
    def start(self, duration=None):
        args = [self.driver.executable]
        if duration:
            args.extend(["-d", "%d" % duration])
            self.duration = duration
            self.expected_shutdown = time.time() + duration - 5
        else:
            self.expected_shutdown = self.duration = None
        
        if self.driver.format:
            args.extend(["-f", self.driver.format])
        if self.driver.bitrate:
            args.extend(["-b", "%d" % self.driver.bitrate])
        if self.driver.channels:
            args.extend(["-c", "%d" % self.driver.channels])
        if self.driver.ports:
            args.extend(["-p", ','.join(self.driver.ports)]);
        if self.driver.client_name:
            args.extend(["-n", self.driver.client_name])
        
        self.output_path = self._get_output_path()
        
        args.append(self.output_path)
        
        self.start_time = time.time()
        black_hole = os.devnull or "/dev/null"
        self.stdin = open(black_hole, "r")
        self.stdout = open(black_hole, "w")
        
        try:
            self._process = subprocess.Popen(args, stdin=self.stdin,
                stdout=self.stdout, stderr=subprocess.STDOUT)
            self._ended = self._stopped = False
            self.fire("start", session=self, process=self._process,
                duration=duration)
            monitor_process(self._process, self._proc_ended)
        except Exception, e:
            self.fire("error", session=self,
                error="failed to start recording: %s" % e)
    
    def stop(self):
        if self._ended:
            raise RuntimeError("cannot stop Jackoff process; process is not "
                "running")
        
        self._stopped = True
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
        
        ended_early = (not self._stopped and self.expected_shutdown and
            time.time() < self.expected_shutdown)
        
        if return_code != 0:
            self.fire("error", session=self, error="jackoff exited with "
                "status %d after %s" % (return_code, elapsed_time))
        elif ended_early:
            expected_shutdown = time.strftime("%Hh%Mm%Ss",
                self.gmtime(self.duration))
            self.fire("error", session=self, error="jackoff exited early, "
                "after only %s (expected %s)" %
                (elapsed_time, expected_shutdown))
        else:
            self.fire("done", session=self, filename=self.output_path)
    
    def _close_streams(self):
        try:
            self.stdin.close()
            self.stdout.close()
        except Exception:
            pass

Driver = JackoffDriver
