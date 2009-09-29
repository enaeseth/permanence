# encoding: utf-8

"""
Implements a storage driver that saves recordings via SFTP.
"""

from __future__ import with_statement

from permanence.config import ConfigurationError
from permanence.event import EventSource
from permanence.storage.util import ActionQueue, compile_path_pattern

import paramiko
import os.path
import posixpath

class SFTPDriver(EventSource):
    def __init__(self, host, path_creator, username, password=None, key=None):
        super(SFTPDriver, self).__init__()
        
        self.host = host[0]
        self.port = host[1]
        self.path_creator = path_creator
        self.username = username
        self.password = password
        self.key = key
        
        self._queue = ActionQueue(self._upload)
    
    @classmethod
    def from_config(cls, config):
        for key in ('host', 'remote_path', 'username'):
            if key not in config:
                raise ConfigurationError("invalid SFTP storage driver "
                    'configuration: no "%s" field provided' % key)
        
        try:
            creator = compile_path_pattern(config["remote_path"])
        except ValueError, e:
            raise ConfigurationError("invalid remote SFTP storage path: "
                "%s" % e)
        
        host = (config['host'], int(config.get('port', 22)))
        return cls(host, creator, config['username'], config.get('password'),
            config.get('key_file'))
    
    def save(self, source, show, file_path):
        extension = os.path.splitext(file_path)[1]
        dest_filename = self.path_creator(source, show) + extension
        
        self._queue.add((source, show, file_path, dest_filename))
    
    def shutdown(self):
        self._queue.shutdown()
    
    def _upload(self, item):
        source, show, source_path, dest_path = item
        
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        
        try:
            client.connect(self.host, self.port, self.username, self.password,
                key_filename=self.key, timeout=15.0, look_for_keys=True)
        except Exception, e:
            self.fire("error", source=source, show=show, error=e)
            return
        
        sftp = client.open_sftp()
        try:
            self._ensure_path(sftp, dest_path)
            sftp.put(source_path, dest_path)
        except Exception, e:
            self.fire("error", source=source, show=show, error=e)
            return
        
        client.close()
        self.fire("save", source=source, show=show, location="%s:%s" %
            (self.host, dest_path))
    
    def _ensure_path(self, sftp, dest_path):
        directory, filename = posixpath.split(dest_path)
        try:
            sftp.chdir(directory)
            return True
        except IOError:
            try:
                parent, child = posixpath.split(directory)
                sftp.chdir(parent)
            except IOError:
                if self._ensure_path(sftp, directory):
                    sftp.mkdir(directory)
                    return True
            else:
                sftp.mkdir(child)
                return True
            
            raise

Driver = SFTPDriver
