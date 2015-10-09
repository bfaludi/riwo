from __future__ import absolute_import
import os
import dm
import yaml
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
        data = yaml.load(content)
        return dm.Mapper(data, routes = {'root': self.route}).root or []

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, input_schema=None, root=None):
        self.root = root
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
        yaml.safe_dump(unmarshaled_data, self.resource, default_flow_style = False)