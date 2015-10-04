from __future__ import absolute_import
import os
import dm
import json
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
        self.route = os.path.join(route or '', '!')
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
    PP_PARAMS = {'sort_keys': False, 'indent': 2, 'separators': (u',', u': ') }

    # void
    def __init__(self, resource, iterable_data, schema=None, not_convert=False, root=None, pretty_print=False):
        self.root = root
        self.pretty_print = pretty_print
        super(Writer, self).__init__(resource, iterable_data, schema, not_convert)

    # void
    def write(self):
        data = self.read() \
            if not self.root \
            else { self.root:self.read() }

        json_data = json.dumps(data, ensure_ascii=False, **(self.PP_PARAMS if self.pretty_print else {}))
        self.resource.write(decode(json_data, self.encoding))