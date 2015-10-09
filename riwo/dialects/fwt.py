from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter,
    exceptions,
)
from ..utils import *
from ..compat import *

class Reader(AbstractReader):
    # Iterable
    def get_iterable_data(self):
        return (decode(item, self.encoding) for item in to_iterable(self.resource))

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, schema=None, add_header=True):
        self.add_header = add_header
        super(Writer, self).__init__(resource, iterable_data, schema)

        if self.is_nested():
            raise exceptions.NestedSchemaNotSupported("{self} is not support nested schemas." \
                .format(self=self.name))

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
            self.resource.write(u'\n')

        for item in items:
            self.write_item(item, length)
            self.resource.write(u'\n')

    # void
    def write_item(self, item, length):
        for name in self.fieldnames:
            self.resource.write(decode(item[name], self.encoding).ljust(length[name]+1))
