# -*- coding: utf-8 -*-

from __future__ import absolute_import
import os
import io
import riwo
import daprot as dp
import datetime
import unittest
import requests
from riwo.compat import *
from . import (
    CommonReader, 
    CommonWriter, 
    __dir__, 
    __remote__
)

class IndexSchema(dp.SchemaFlow):
    id = dp.Field(0)
    name = dp.Field(1, type=unicode, transforms=unicode.strip)
    price = dp.Field(2)
    quantity = dp.Field(3, type=long)
    updated_at = dp.Field(4)

class HeaderedSchema(dp.SchemaFlow):
    id = dp.Field()
    name = dp.Field(type=unicode, transforms=unicode.strip)
    price = dp.Field()
    quantity = dp.Field(type=long)
    updated_at = dp.Field()

class LocalReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test.xlsx'), 'rb')
        self.reader = riwo.excel.Reader(self.resource, IndexSchema, sheet='Sheet1')

class RequestsReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test.xlsx'))
        self.reader = riwo.excel.Reader(self.resource, IndexSchema, sheet='Sheet1')

class UrllibReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test.xlsx'))
        self.reader = riwo.excel.Reader(self.resource, IndexSchema, sheet='Sheet1')

class LocalWriter(CommonWriter, unittest.TestCase):
    class Schema(dp.SchemaFlow):
        id = dp.Field()
        name = dp.Field()
        price = dp.Field()
        quantity = dp.Field()
        updated_at = dp.Field()

    class NestedSchema(dp.SchemaFlow):
        inner_schema = dp.DictOf(IndexSchema)

    ext = 'xlsx'

    def test_add_header(self):
        self.writer = riwo.excel.Writer(self.resource, self.iterable_input, self.Schema, add_header=True)
        self.writer.write()
        self.content = u'''\
id,name,price,quantity,updated_at
P0001,đói,449,1,2015.09.20 20:00
P0002,배고픈,399,1,2015.09.20 20:02
P0003,голодный,199,10,
P0004,Űrállomás krízis,"999,5",1,2015.09.20 12:47
P0005,Ovális iroda,2 399,1,2015.09.20 07:31
'''

    def test_not_add_header(self):
        self.writer = riwo.excel.Writer(self.resource, self.iterable_input, self.Schema, add_header=False)
        self.writer.write()
        self.content = u'''\
P0001,đói,449,1,2015.09.20 20:00
P0002,배고픈,399,1,2015.09.20 20:02
P0003,голодный,199,10,
P0004,Űrállomás krízis,"999,5",1,2015.09.20 12:47
P0005,Ovális iroda,2 399,1,2015.09.20 07:31
'''

    def test_defined_schema_writer(self):
        self.writer = riwo.excel.Writer(self.resource, self.iterable_input, self.Schema)
        self.writer.write()
        self.content = u'''\
id,name,price,quantity,updated_at
P0001,đói,449,1,2015.09.20 20:00
P0002,배고픈,399,1,2015.09.20 20:02
P0003,голодный,199,10,
P0004,Űrállomás krízis,"999,5",1,2015.09.20 12:47
P0005,Ovális iroda,2 399,1,2015.09.20 07:31
'''

    def test_schema_flow_writer(self):
        self.writer = riwo.excel.Writer(self.resource, self.Schema(self.iterable_input, mapper=dp.mapper.NAME))
        self.writer.write()
        self.content = u'''\
id,name,price,quantity,updated_at
P0001,đói,449,1,2015.09.20 20:00
P0002,배고픈,399,1,2015.09.20 20:02
P0003,голодный,199,10,
P0004,Űrállomás krízis,"999,5",1,2015.09.20 12:47
P0005,Ovális iroda,2 399,1,2015.09.20 07:31
'''

    def test_without_schema(self):
        with self.assertRaises(riwo.exceptions.SchemaRequired):
            riwo.excel.Writer(self.resource, self.iterable_input)
        self.content = u''

    def test_unmarshal(self):
        self.writer = riwo.excel.Writer(self.resource, [], self.Schema)
        self.content = u''

        current_datetime = datetime.datetime.now()
        self.assertEqual(self.writer.unmarshal_item(current_datetime), current_datetime.isoformat())
        self.assertEqual(self.writer.unmarshal_item(4.21), u'4.21')
        self.assertEqual(self.writer.unmarshal_item(u'голодный'), u'голодный')

    def test_requisites(self):
        with self.assertRaises(riwo.exceptions.NestedSchemaNotSupported):
             riwo.excel.Writer(self.resource, [], self.NestedSchema)
        self.content = u''
