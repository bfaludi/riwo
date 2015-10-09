from __future__ import absolute_import
import os
import dm
import datetime
import requests
import xmlsquash
import daprot.mapper
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter
)
from ..compat import *
from ..utils import *
from xml.etree import ElementTree
from xml.dom import minidom

class Reader(AbstractReader):
    # void
    def __init__(self, resource, schema, route, offset=0, limit=None):
        self.route = os.path.join(route, u'!')
        super(Reader, self).__init__(resource, schema, offset, limit)

    # Iterable
    def get_iterable_data(self):
        # WARNING: It will contains the whole dataset in memory.
        if isinstance(self.resource, requests.Response):
            data = xmlsquash.XML2Dict().parseString(encode(get_content(self.resource), self.encoding))
        else:
            data = xmlsquash.XML2Dict().parseFile(self.resource)
        return dm.Mapper(data, routes = {'root': self.route}).root or []

class Writer(AbstractWriter):
    # void
    def __init__(self, resource, iterable_data, item_name, input_schema=None, root='root'):
        self.root = root
        self.item_name = item_name
        super(Writer, self).__init__(resource, iterable_data, input_schema)

    # type
    def unmarshal_item(self, item):
        if isinstance(item, (datetime.date, datetime.datetime)):
            return item.isoformat()
        return unicode(item or u'')

    # unicode
    def prettify(self):
        raw_string = ElementTree.tostring(self.tree, self.encoding)
        reparsed = minidom.parseString(raw_string)
        xml_string = reparsed.toprettyxml(indent = 2*u' ')
        return xml_string

    # void
    def write(self):
        self.tree = ElementTree.Element(self.root)
        super(Writer, self).write()
        self.resource.write(self.prettify())

    # void
    def write_item(self, item):
        tree_element = ElementTree.SubElement(self.tree, self.item_name)
        unmarshaled_item = unmarshal(item, self.unmarshal_item)
        for name in self.fieldnames:
            self.add_item(tree_element, name, unmarshaled_item[name])

    # void
    def add_item(self, parent_element, name, value):
        if isinstance(value, list):
            self.add_list(parent_element, name, value)
        elif isinstance(value, dict):
            self.add_dict(parent_element, name, value)
        else:
            element = ElementTree.SubElement(parent_element, name)
            element.text = value

    # void
    def add_list(self, parent_element, name, values):
        for value in values:
            self.add_item(parent_element, name, value)

    def add_dict(self, parent_element, name, value):
        element = ElementTree.SubElement(parent_element, name)
        for i_name, i_value in value.items():
            self.add_item(element, i_name, i_value)