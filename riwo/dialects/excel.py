from __future__ import absolute_import
import csv
import daprot.mapper
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter,
    exceptions
)
from io import BytesIO
from ..utils import *
from ..compat import *
from openpyxl import load_workbook

class Reader(AbstractReader):
    # void
    def __init__(self, resource, schema, offset=0, limit=None, sheet=None,  **fmtparams):
        self.fmtparams = fmtparams
        self.sheet = sheet
        super(Reader, self).__init__(resource, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.NAME

    def get_iterable_data(self):
        wb = load_workbook(
            filename=BytesIO(encode(get_content(self.resource), self.encoding)),
            use_iterators= True,
            guess_types=False,
            data_only=True,
            read_only=True
        )
        ws = wb[self.sheet] if self.sheet else wb.active

        result_gen = ([unicode(c.value or u'') for c in r] for r in ws.rows)
        return result_gen

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, input_schema=None, add_header=True, **fmtparams):
        self.fmtparams = fmtparams
        self.add_header = add_header
        super(Writer, self).__init__(resource, iterable_data, input_schema)

        if self.is_nested():
            raise exceptions.NestedSchemaNotSupported("{self} is not support nested schemas." \
                .format(self=self.name))

    # csv.DictWriter
    def init_writer(self):
        return load_workbook(self.resource, read_only=False)

    # str in PY3 and unicode in PY2
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()

        # Convert to string.
        return unicode(item or u'')

    # void
    def write_header(self):
        self.writer.append(self.fieldnames)

    # void
    def write(self):
        if self.add_header: self.write_header()
        super(Writer, self).write()

    # void
    def write_item(self, item):
        data = unmarshal(item, self.unmarshal_item)
        self.writer.append([data[c] for c in self.fieldnames])


