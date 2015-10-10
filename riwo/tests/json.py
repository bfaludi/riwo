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

class Schema(dp.SchemaFlow):
    id = dp.Field()
    name = dp.Field(type=unicode, transforms=unicode.strip)
    price = dp.Field()
    quantity = dp.Field(type=long)
    updated_at = dp.Field()

class LocalRootReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-root.json'), 'r', encoding='utf-8')
        self.reader = riwo.json.Reader(self.resource, Schema)

class LocalRouteReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-route.json'), 'r', encoding='utf-8')
        self.reader = riwo.json.Reader(self.resource, Schema, route='response/items')

class RequestsRootReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test-root.json'))
        self.reader = riwo.json.Reader(self.resource, Schema)

class RequestsRouteReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test-route.json'))
        self.reader = riwo.json.Reader(self.resource, Schema, route='response/items')

class UrllibRootReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test-root.json'))
        self.reader = riwo.json.Reader(self.resource, Schema)

class UrllibRouteReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test-route.json'))
        self.reader = riwo.json.Reader(self.resource, Schema, route='response/items')

class LocalFlatWriter(CommonWriter, unittest.TestCase):
    class Schema(dp.SchemaFlow):
        id = dp.Field()
        name = dp.Field()
        price = dp.Field()
        quantity = dp.Field()
        updated_at = dp.Field()

    ext = 'json'

    def test_pretty_print(self):
        self.writer = riwo.json.Writer(self.resource, self.iterable_input, Schema, pretty_print=True, sort_keys=True)
        self.writer.write()
        self.content = u'''\
[
  {
    "id": "P0001",
    "name": "đói",
    "price": "449",
    "quantity": 1,
    "updated_at": "2015.09.20 20:00"
  },
  {
    "id": "P0002",
    "name": "배고픈",
    "price": "399",
    "quantity": 1,
    "updated_at": "2015.09.20 20:02"
  },
  {
    "id": "P0003",
    "name": "голодный",
    "price": "199",
    "quantity": 10,
    "updated_at": ""
  },
  {
    "id": "P0004",
    "name": "Űrállomás krízis",
    "price": "999,5",
    "quantity": 1,
    "updated_at": "2015.09.20 12:47"
  },
  {
    "id": "P0005",
    "name": "Ovális iroda",
    "price": "2 399",
    "quantity": 1,
    "updated_at": "2015.09.20 07:31"
  }
]'''

    def test_root(self):
        self.writer = riwo.json.Writer(self.resource, self.iterable_input, Schema, root="items", sort_keys=True)
        self.writer.write()
        self.content = u'''\
{"items": [{"id": "P0001", "name": "đói", "price": "449", "quantity": 1, "updated_at": "2015.09.20 20:00"}, {"id": "P0002", "name": "배고픈", "price": "399", "quantity": 1, "updated_at": "2015.09.20 20:02"}, {"id": "P0003", "name": "голодный", "price": "199", "quantity": 10, "updated_at": ""}, {"id": "P0004", "name": "Űrállomás krízis", "price": "999,5", "quantity": 1, "updated_at": "2015.09.20 12:47"}, {"id": "P0005", "name": "Ovális iroda", "price": "2 399", "quantity": 1, "updated_at": "2015.09.20 07:31"}]}'''

    def test_defined_schema_writer(self):
        self.writer = riwo.json.Writer(self.resource, self.iterable_input, Schema, sort_keys=True)
        self.writer.write()
        self.content = u'''\
[{"id": "P0001", "name": "đói", "price": "449", "quantity": 1, "updated_at": "2015.09.20 20:00"}, {"id": "P0002", "name": "배고픈", "price": "399", "quantity": 1, "updated_at": "2015.09.20 20:02"}, {"id": "P0003", "name": "голодный", "price": "199", "quantity": 10, "updated_at": ""}, {"id": "P0004", "name": "Űrállomás krízis", "price": "999,5", "quantity": 1, "updated_at": "2015.09.20 12:47"}, {"id": "P0005", "name": "Ovális iroda", "price": "2 399", "quantity": 1, "updated_at": "2015.09.20 07:31"}]'''

    def test_schema_flow_writer(self):
        self.writer = riwo.json.Writer(self.resource, self.Schema(self.iterable_input, mapper=dp.mapper.NAME), sort_keys=True)
        self.writer.write()
        self.content = u'''\
[{"id": "P0001", "name": "đói", "price": "449", "quantity": 1, "updated_at": "2015.09.20 20:00"}, {"id": "P0002", "name": "배고픈", "price": "399", "quantity": 1, "updated_at": "2015.09.20 20:02"}, {"id": "P0003", "name": "голодный", "price": "199", "quantity": 10, "updated_at": ""}, {"id": "P0004", "name": "Űrállomás krízis", "price": "999,5", "quantity": 1, "updated_at": "2015.09.20 12:47"}, {"id": "P0005", "name": "Ovális iroda", "price": "2 399", "quantity": 1, "updated_at": "2015.09.20 07:31"}]'''

    def test_without_schema(self):
        with self.assertRaises(riwo.exceptions.SchemaRequired):
            riwo.json.Writer(self.resource, self.iterable_input)
        self.content = u''

    def test_unmarshal(self):
        self.writer = riwo.json.Writer(self.resource, [], Schema)
        self.content = u''

        current_datetime = datetime.datetime.now()
        self.assertEqual(self.writer.unmarshal_item(current_datetime), current_datetime.isoformat())
        self.assertEqual(self.writer.unmarshal_item(4.21), 4.21)
        self.assertEqual(self.writer.unmarshal_item(u'голодный'), u'голодный')
        self.assertEqual(self.writer.unmarshal_item(True), True)

class LocalNestedWriter(CommonWriter, unittest.TestCase):
    class NestedSchema(dp.SchemaFlow):
        id = dp.Field()
        name = dp.Field()
        price = dp.Field()
        quantity = dp.Field()
        updated_at = dp.Field()

    ext = 'json'
    pass