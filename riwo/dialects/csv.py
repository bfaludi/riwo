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

class Py3Reader(AbstractReader):
    # void
    def __init__(self, resource, schema, offset=0, limit=None, use_header=False, **fmtparams):
        self.fmtparams = fmtparams
        self.use_header = use_header
        super(Py3Reader,self).__init__(resource, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.INDEX \
            if not self.use_header \
            else daprot.mapper.NAME

    # Iterable (csv.reader or csv.DictReader)
    def get_iterable_data(self):
        resource_gen = (decode(r, self.encoding) for r in to_iterable(self.resource))
        return csv.reader(resource_gen, **self.fmtparams) \
            if not self.use_header \
            else csv.DictReader(resource_gen, **self.fmtparams)

class Py2Reader(AbstractReader):
    # void
    def __init__(self, resource, schema, offset=0, limit=None, use_header=False, **fmtparams):
        self.fmtparams = fmtparams
        self.use_header = use_header
        super(Py2Reader,self).__init__(resource, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.INDEX \
            if not self.use_header \
            else daprot.mapper.NAME

    # Iterable
    def get_iterable_data(self):
        resource_gen = (encode(r, self.encoding) for r in to_iterable(self.resource))
        reader_gen = csv.reader(resource_gen, **self.fmtparams)
        result_gen = ( [unicode(c, self.encoding) for c in r] for r in reader_gen )
        if not self.use_header:
            return result_gen

        header = result_gen.next()
        return (dict(zip(header,r)) for r in result_gen)

class Py3Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, input_schema=None, add_header=True, **fmtparams):
        self.fmtparams = fmtparams
        self.add_header = add_header
        super(Py3Writer, self).__init__(resource, iterable_data, input_schema)

        if self.is_nested():
            raise exceptions.NestedSchemaNotSupported("{self} is not support nested schemas." \
                .format(self=self.name))

    # csv.DictWriter
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
        super(Py3Writer, self).write()

    # void
    def write_item(self, item):
        self.writer.writerow(unmarshal(item, self.unmarshal_item))

class Py2Writer(Py3Writer):
    # UnicodeWriter
    def init_writer(self):
        return UnicodeWriter(self.resource, **self.fmtparams)

    # void
    def write_header(self):
        self.writer.writerow(self.fieldnames)

    # void
    def write(self):
        if self.add_header: self.write_header()
        for item in self.reader:
            self.write_item(item)

    # void
    def write_item(self, item):
        data = unmarshal(item, self.unmarshal_item)
        self.writer.writerow([data[c] for c in self.fieldnames])

class UnicodeWriter:
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = getencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([encode(s, "utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = decode(data, "utf-8")
        # ... and reencode it into the target encoding
        # data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

if PY3:
    Reader = Py3Reader
    Writer = Py3Writer
else:
    Reader = Py2Reader
    Writer = Py2Writer
