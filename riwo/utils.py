import datetime
from collections import Iterable

# Iterable
def to_iterable(resource):
    if isinstance(resource, Iterable):
        return resource
    elif hasattr(resource, 'xreadlines'):
        return resource.xreadlines()
    elif hasattr(resource, 'readlines'):
        return resource.readlines()
    elif hasattr(resource, 'read'):
        return get_content(resource).split(u'\n')

    raise AttributeError('Unable to convert `resource to iterable.')

# str or bytes
def get_content(resource):
    if hasattr(resource, 'read'):
        return resource.read()

    raise AttributeError('Unable to read the resource! Pleade define `read` function.')

# type
def unmarshal(data, unmarshal_item_fn):
    if isinstance(data, list):
        return [ unmarshal(item, unmarshal_item_fn) for item in data ]
    elif isinstance(data, dict):
        return { unmarshal(k, unmarshal_item_fn) : unmarshal(item, unmarshal_item_fn) for k,item in data.items() }
    else:
        return unmarshal_item_fn(data)