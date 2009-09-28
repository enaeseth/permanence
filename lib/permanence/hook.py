# encoding: utf-8

"""
Hooks allow external code to be notified of Permanence events.

Permanence transparently supports two kinds of hook implementations: named
Python callables and executable files. Python callables are called with any
hook data as keyword arguments and can return data to their caller, if the
caller accepts returned data from hooks. When external programs, the hook
arguments are encoded as a JSON object and passed to the program's standard
input. If the program wishes to return a value, it should encode that value
in JSON and write it to standard output.
"""

try:
    import simplejson as json
except ImportError:
    import json

import subprocess
import sys
import os
import os.path
import re

def get_hook(name, exec_search_path):
    """
    Returns a callable that will invoke the hook with the given name.
    
    The name may refer to a callable in a Python module or an executable file.
    If the name is not an absolute path, executables will be searched for in
    the given search path.
    
    If no such hook can be found, returns None.
    """
    
    def get_exec_hook(path):
        if not os.path.isfile(path):
            return None
        if not os.access(path, os.X_OK):
            return None
        return ExternalScriptHook(path)
    
    if re.match(r'^(/|\\\\|[A-Za-z]:\\)', name):
        # absolute path
        return get_exec_hook(name)
    
    if not exec_search_path:
        exec_search_path = []
    
    for search_dir in exec_search_path:
        path = os.path.join(search_dir, name)
        hook = get_exec_hook(path)
        if hook:
            return hook
    
    # check for a Python hook
    parts = name.split('.')
    if len(parts) <= 1:
        return None
    
    module, hook = '.'.join(parts[:-1]), parts[-1]
    old_path = sys.path[:]
    for path in reversed(exec_search_path):
        sys.path.insert(0, path)
    try:
        module = __import__(module, globals(), locals())
        return getattr(module, hook)
    except (ImportError, AttributeError):
        return None
    finally:
        sys.path = old_path

class HookExecutionError(RuntimeError):
    pass

class ExternalScriptHook(object):
    def __init__(self, path):
        self.path = path
    
    def __call__(self, **kwargs):
        if len(kwargs) > 0:
            try:
                data = json.dumps(kwargs, cls=PermanenceJSONEncoder)
            except (TypeError, ValueError), e:
                raise HookExecutionError("failed to serialize hook arguments: "
                    "%s" % e)
        else:
            data = None
        
        args = [os.path.basename(self.path)]
        working = os.path.dirname(self.path)
        try:
            process = subprocess.Popen(args, executable=self.path,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, shell=True, cwd=working)
            output, error_output = process.communicate(data)
            
            if process.returncode != 0:
                raise HookExecutionError("hook exited with error status %d" %
                    process.returncode)
            
            try:
                return json.loads(output)
            except ValueError:
                return None
        except OSError, e:
            raise HookExecutionError("failed to execute hook: %s" % e.message)
    
    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.path)

class PermanenceJSONEncoder(json.JSONEncoder):
    encoders = []
    
    def default(self, obj):
        for custom_type, encoder in self.encoders:
            if isinstance(obj, custom_type):
                return encoder(obj)
        return json.JSONEncoder.default(obj)

def add_json_serializer(custom_type, encoder):
    PermanenceJSONEncoder.encoders.append((custom_type, encoder))
