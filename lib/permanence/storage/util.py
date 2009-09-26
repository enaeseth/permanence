# encoding: utf-8

"""
Utility code for storage drivers.
"""

def compile_path_pattern(pattern):
    def path_formatter(fn):
        def path_format(source, show):
            return re.sub(r'\W+', '', re.sub(r'\s+', '_',
                fn(source, show))).lower()
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
