from __future__ import absolute_import
import csv
import daprot.mapper
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter,
    exceptions
)
from ..utils import *
from ..compat import *
from csv import (
    QUOTE_MINIMAL,
    QUOTE_ALL,
    QUOTE_NONNUMERIC,
    QUOTE_NONE
)

if PY3:
    code=decode
else:
    code=encode

# TODO: Python 2.7 compatibility is missing. (encoding is failing)

class Reader(AbstractReader):
    # void
    def __init__(self, resource, schema, offset=0, limit=None, use_header=False, **fmtparams):
        self.fmtparams = fmtparams
        self.use_header = use_header
        super(Reader,self).__init__(resource, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.INDEX \
            if not self.use_header \
            else daprot.mapper.NAME

    # Iterable (csv.reader or csv.DictReader)
    def get_iterable_data(self):
        resource_gen = (code(r, self.encoding) for r in to_iterable(self.resource)) # csv package can't open bytestream
        return csv.reader(resource_gen, **self.fmtparams) \
            if not self.use_header \
            else csv.DictReader(resource_gen, **self.fmtparams)


class Writer(AbstractWriter):

    # void
    def __init__(self, resource, iterable_data, schema=None, not_convert=False, add_header=True, **fmtparams):
        self.fmtparams = fmtparams
        self.add_header = add_header
        self.resource = resource
        self.write_resource = self.resource if PY3 else StringIO()
        super(Writer, self).__init__(resource, iterable_data, schema, not_convert)

        if self.is_nested():
            raise exceptions.NestedSchemaNotSupported("{self} is not support nested schemas." \
                .format(self=self.name))

    # csv.writer
    def init_writer(self):

        return csv.writer(self.write_resource, **self.fmtparams)

    # str in PY3 and unicode in PY2
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()

        # Convert to string.
        return str(item or u'')

    # void
    def write_stream(self):
        if PY3:
            return

        data = self.write_resource.getvalue()
        data = decode(data, self.encoding)
        self.resource.write(data)
        self.write_resource.truncate(0)

    # void
    def write_row(self, row):
        self.writer.writerow(row)
        self.write_stream()

    # void
    def write_header(self):
        self.write_row([code(f, self.encoding) for f in self.fieldnames])

    # void
    def write_item(self, item):
        data = unmarshal(item, self.unmarshal_item)
        self.write_row([code(data.get(f, u''), self.encoding) for f in self.fieldnames])

    # void
    def write(self):
        if self.add_header: self.write_header()
        super(Writer, self).write()