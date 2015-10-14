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
from openpyxl import *

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
        workbook = load_workbook(filename=BytesIO(encode(get_content(self.resource), self.encoding)))
        worksheet = workbook[self.sheet] if self.sheet else workbook.active

        result_gen = ([unicode(c.value or u'') for c in r] for r in worksheet.rows)
        return result_gen

class Writer(AbstractWriter):
    # void
    def __init__(self, filename, iterable_data, input_schema=None, add_header=True, sheet=None, **fmtparams):
        self.fmtparams = fmtparams
        self.add_header = add_header
        self.sheet = sheet
        super(Writer, self).__init__(filename, iterable_data, input_schema)

        if self.is_nested():
            raise exceptions.NestedSchemaNotSupported("{self} is not support nested schemas." \
                .format(self=self.name))

    # tuple
    def init_writer(self):
        workbook = Workbook()
        worksheet = workbook[self.sheet] if self.sheet else workbook.active

        return (workbook, worksheet)

    # str in PY3 and unicode in PY2
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()

        # Convert to string.
        return unicode(item or u'')

    # void
    def write_header(self):
        self.writer[1].append(self.fieldnames)

    # void
    def write(self):
        if self.add_header: self.write_header()
        super(Writer, self).write()
        self.writer[0].save(self.resource)

    # void
    def write_item(self, item):
        data = unmarshal(item, self.unmarshal_item)
        self.writer[1].append([data[c] for c in self.fieldnames])


