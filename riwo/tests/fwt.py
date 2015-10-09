# -*- coding: utf-8 -*-

from __future__ import absolute_import
import io
import os
import riwo
import daprot as dp
import string
import random
import datetime
import unittest
import requests
from riwo.compat import *

__dir__ = os.path.join(os.path.dirname(__file__), 'source')
__remote__ = 'https://raw.githubusercontent.com/bfaludi/riwo/master/riwo/tests/source'

class Schema(dp.SchemaFlow):
    id = dp.Field('0:8')
    name = dp.Field('8:25', type=unicode, transforms=unicode.strip)
    price = dp.Field('25:34')
    quantity = dp.Field('34:40', type=long)
    updated_at = dp.Field('40:')

class CommonReader(object):
    expected_result = [{
      u'quantity': 1,
      u'name': u'đói',
      u'updated_at': u'2015.09.20 20:00',
      u'price': u'449',
      u'id': u'P0001'
    }, {
      u'quantity': 1,
      u'name': u'배고픈',
      u'updated_at': u'2015.09.20 20:02',
      u'price': u'399',
      u'id': u'P0002'
    }, {
      u'quantity': 10,
      u'name': u'голодный',
      u'updated_at': u'',
      u'price': u'199',
      u'id': u'P0003'
    }, {
      u'quantity': 1,
      u'name': u'Űrállomás krízis',
      u'updated_at': u'2015.09.20 12:47',
      u'price': u'999,5',
      u'id': u'P0004'
    }, {
      u'quantity': 1,
      u'name': u'Ovális iroda',
      u'updated_at': u'2015.09.20 07:31',
      u'price': u'2 399',
      u'id': u'P0005'
    }]

    def test_stored_value(self):
        self.assertEqual(list(self.reader), self.expected_result)

    def test_iterated_value(self):
        idx = 0
        for item in self.reader:
            self.assertEqual(item, self.expected_result[idx])
            idx += 1

    def test_length(self):
        self.assertEqual(len(list(self.reader)), 5)

    def tearDown(self):
        self.resource.close()

class LocalReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test.fwt'), 'r', encoding='utf-8')
        self.reader = riwo.fwt.Reader(self.resource, Schema)

class RequestsReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test.fwt'))
        self.reader = riwo.fwt.Reader(self.resource, Schema)

class UrllibReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test.fwt'))
        self.reader = riwo.fwt.Reader(self.resource, Schema)

class LocalWriter(unittest.TestCase):
    class Schema(dp.SchemaFlow):
        id = dp.Field()
        name = dp.Field()
        price = dp.Field()
        quantity = dp.Field()
        updated_at = dp.Field()

    class NestedSchema(dp.SchemaFlow):
        inner_schema = dp.DictOf(Schema)

    iterable_input = CommonReader.expected_result
    test_file_base = os.path.join(__dir__, 'output-{token}.fwt')

    def setUp(self):
        self.test_file_path = self.test_file_base.format(token=''.join([random.choice(string.ascii_uppercase) for i in range(6)]))
        self.resource = io.open(self.test_file_path, 'w', encoding='utf-8')

    def test_add_header(self):
        self.writer = riwo.fwt.Writer(self.resource, self.iterable_input, Schema, add_header=True)
        self.writer.write()
        self.content = u'''\
id    name             price quantity updated_at       
P0001 đói              449   1        2015.09.20 20:00 
P0002 배고픈              399   1        2015.09.20 20:02 
P0003 голодный         199   10                        
P0004 Űrállomás krízis 999,5 1        2015.09.20 12:47 
P0005 Ovális iroda     2 399 1        2015.09.20 07:31 
'''

    def test_not_add_header(self):
        self.writer = riwo.fwt.Writer(self.resource, self.iterable_input, Schema, add_header=False)
        self.writer.write()
        self.content = u'''\
P0001 đói              449   1  2015.09.20 20:00 
P0002 배고픈              399   1  2015.09.20 20:02 
P0003 голодный         199   10                  
P0004 Űrállomás krízis 999,5 1  2015.09.20 12:47 
P0005 Ovális iroda     2 399 1  2015.09.20 07:31 
'''

    def test_defined_schema_writer(self):
        self.writer = riwo.fwt.Writer(self.resource, self.iterable_input, Schema)
        self.writer.write()
        self.content = u'''\
id    name             price quantity updated_at       
P0001 đói              449   1        2015.09.20 20:00 
P0002 배고픈              399   1        2015.09.20 20:02 
P0003 голодный         199   10                        
P0004 Űrállomás krízis 999,5 1        2015.09.20 12:47 
P0005 Ovális iroda     2 399 1        2015.09.20 07:31 
'''

    def test_schema_flow_writer(self):
        self.writer = riwo.fwt.Writer(self.resource, self.Schema(self.iterable_input, mapper=dp.mapper.NAME))
        self.writer.write()
        self.content = u'''\
id    name             price quantity updated_at       
P0001 đói              449   1        2015.09.20 20:00 
P0002 배고픈              399   1        2015.09.20 20:02 
P0003 голодный         199   10                        
P0004 Űrállomás krízis 999,5 1        2015.09.20 12:47 
P0005 Ovális iroda     2 399 1        2015.09.20 07:31 
'''

    def test_without_schema(self):
        with self.assertRaises(riwo.exceptions.SchemaRequired):
            riwo.fwt.Writer(self.resource, self.iterable_input)
        self.content = u''

    def test_unmarshal(self):
        self.writer = riwo.fwt.Writer(self.resource, [], Schema)
        self.content = u''

        current_datetime = datetime.datetime.now()
        self.assertEqual(self.writer.unmarshal_item(current_datetime), current_datetime.isoformat())
        self.assertEqual(self.writer.unmarshal_item(4.21), u'4.21')
        self.assertEqual(self.writer.unmarshal_item(u'голодный'), u'голодный')

    def test_requisites(self):
        with self.assertRaises(riwo.exceptions.NestedSchemaNotSupported):
             riwo.fwt.Writer(self.resource, [], self.NestedSchema)
        self.content = u''

    def tearDown(self):
        self.resource.close()
        with io.open(self.test_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertEqual(self.content, content)
        os.remove(self.test_file_path)
