import sys
import types

# True if we are running on Python 3.
PY3 = sys.version_info[0] == 3

if PY3: # pragma: no cover
    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes
    long = int
    unicode = str

    import io
    StringIO=io.StringIO

else:
    string_types = basestring,
    integer_types = (int, long)
    class_types = (type, types.ClassType)
    text_type = unicode
    binary_type = str
    long = long

    import cStringIO
    StringIO=cStringIO.StringIO

# str in PY3 & unicode in PY2
def decode(text, encoding):
    if isinstance(text, binary_type):
        return text.decode(encoding)
    return text

# bytes in PY3 & str in PY2
def encode(text, encoding):
    if not isinstance(text, binary_type):
        return text.encode(encoding)
    return text
