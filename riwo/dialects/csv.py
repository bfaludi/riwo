from __future__ import absolute_import
import csv
import daprot.mapper
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter
)
from ..utils import *
from ..compat import *
from csv import (
    QUOTE_MINIMAL,
    QUOTE_ALL,
    QUOTE_NONNUMERIC,
    QUOTE_NONE
)

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
        resource_gen = (decode(r, self.encoding) for r in to_iterable(self.resource)) # csv package can't open bytestream
        return csv.reader(resource_gen, **self.fmtparams) \
            if not self.use_header \
            else csv.DictReader(resource_gen, **self.fmtparams)

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, schema=None, not_convert=False, add_header=True, **fmtparams):
        self.fmtparams = fmtparams
        self.add_header = add_header
        super(Writer, self).__init__(resource, iterable_data, schema, not_convert)

    # csv.writer
    def init_writer(self):
        return csv.DictWriter(self.resource, fieldnames=self.fieldnames, **self.fmtparams)

    # str in PY3 and unicode in PY2
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()

        # Convert to string.
        return unicode(item or u'')

    # void
    def write(self):
        if self.add_header: self.writer.writeheader()
        super(Writer, self).write()

    # void
    def write_item(self, item):
        self.writer.writerow(unmarshal(item, self.unmarshal_item))
