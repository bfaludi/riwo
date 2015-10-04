from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter
)
from ..utils import *
from ..compat import *

class Reader(AbstractReader):
    # Iterable
    def get_iterable_data(self):
        return to_iterable(self.resource)

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, schema=None, not_convert=False, add_header=True):
        self.add_header = add_header
        super(Writer, self).__init__(resource, iterable_data, schema, not_convert)

    # str in PY3 and unicode in PY2
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()

        # Convert to string.
        return unicode(item or u'')

    # list<int>
    def get_fields_maximum_length(self, items):
        return { name:max(len(item[name]) for item in items) for name in self.fieldnames }

    # list<int>
    def get_header_maximum_length(self):
        return { name:len(name) for name in self.fieldnames }

    # list<int>
    def get_lengths(self, items):
        field_length = self.get_fields_maximum_length(items)
        if not self.add_header: return field_length
        header_length = self.get_header_maximum_length()
        return { name:max(field_length[name], header_length[name]) for name in self.fieldnames }

    # void
    def write(self):
        items = unmarshal(self.read_items(), self.unmarshal_item)
        length = self.get_lengths(items)
        fieldnames = self.fieldnames

        if self.add_header:
            self.write_item(dict(zip(fieldnames, fieldnames)), length)

        for item in items:
            self.write_item(item, length)

    # void
    def write_item(self, item, length):
        line = u''
        for name in self.fieldnames:
            line += item[name].ljust(length[name]+1)
        self.resource.write(line + '\n')
