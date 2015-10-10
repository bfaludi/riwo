from __future__ import absolute_import
import os
import dm
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
    # void
    def __init__(self, resource, schema, offset=0, limit=None, route=None):
        self.route = os.path.join(route or u'', u'!')
        super(Reader, self).__init__(resource, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.NAME

    # Iterable
    def get_iterable_data(self):
        # WARNING: It will contains the whole dataset in memory.
        content = decode(get_content(self.resource), self.encoding)
        json_data = json.loads(content)
        return dm.Mapper(json_data, routes = {'root': self.route}).root or []

class Writer(AbstractWriter):
    PP_PARAMS = {'indent': 2, 'separators': (u',', u': ') }

    # void
    def __init__(self, resource, iterable_data, input_schema=None, root=None, pretty_print=False, sort_keys=False):
        self.root = root
        self.pretty_print = pretty_print
        self.sort_keys = sort_keys
        super(Writer, self).__init__(resource, iterable_data, input_schema)

    # type
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()
        return item

    # void
    def write(self):
        data = self.read_items() \
            if not self.root \
            else { self.root:self.read_items() }
        unmarshaled_data = unmarshal(data, self.unmarshal_item)

        json_data = json.dumps(unmarshaled_data, sort_keys=self.sort_keys, ensure_ascii=False, \
                               **(self.PP_PARAMS if self.pretty_print else {}))
        self.resource.write(decode(json_data, self.encoding))