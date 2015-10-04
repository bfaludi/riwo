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