from __future__ import absolute_import
import json
import datetime
import daprot.mapper
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter
)
from ..compat import *
from ..utils import *

class Reader(AbstractReader):
    # function
    def get_mapper(self):
        return daprot.mapper.NAME

    # Iterable
    def get_iterable_data(self):
        return ( json.loads(decode(r, self.encoding)) \
            for r in to_iterable(self.resource) ) # json package can't open bytestream

class Writer(AbstractWriter):
    # type
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()
        return item

    # void
    def write_item(self, item):
        unmarshaled_data = unmarshal(item, self.unmarshal_item)
        json_data = json.dumps(unmarshaled_data, ensure_ascii=False)
        self.resource.write(decode(json_data, self.encoding))
        self.resource.write(u'\n')