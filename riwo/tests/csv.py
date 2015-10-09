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
from . import CommonReader, __dir__, __remote__

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
        self.resource = io.open(os.path.join(__dir__, 'test.csv'), 'r', encoding='utf-8')
        self.reader = riwo.csv.Reader(self.resource, IndexSchema, delimiter=';')

class LocalHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-header.csv'), 'r', encoding='utf-8')
        self.reader = riwo.csv.Reader(self.resource, HeaderedSchema, use_header=True)

class RequestsReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema, delimiter=';')

class RequestsHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test-header.csv'))
        self.reader = riwo.csv.Reader(self.resource, HeaderedSchema, use_header=True)

class UrllibReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema, delimiter=';')

class UrllibHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test-header.csv'))
        self.reader = riwo.csv.Reader(self.resource, HeaderedSchema, use_header=True)

class LocalWriter(unittest.TestCase):
    class Schema(dp.SchemaFlow):
        id = dp.Field()
        name = dp.Field()
        price = dp.Field()
        quantity = dp.Field()
        updated_at = dp.Field()

    class NestedSchema(dp.SchemaFlow):
        inner_schema = dp.DictOf(IndexSchema)

    iterable_input = CommonReader.expected_result
    test_file_base = os.path.join(__dir__, 'output-{token}.csv')

    def setUp(self):
        self.test_file_path = self.test_file_base.format(token=''.join([random.choice(string.ascii_uppercase) for i in range(6)]))
        self.resource = io.open(self.test_file_path, 'w', encoding='utf-8')

    def test_add_header(self):
        self.writer = riwo.csv.Writer(self.resource, self.iterable_input, self.Schema, add_header=True)
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
        self.writer = riwo.csv.Writer(self.resource, self.iterable_input, self.Schema, add_header=False)
        self.writer.write()
        self.content = u'''\
P0001,đói,449,1,2015.09.20 20:00
P0002,배고픈,399,1,2015.09.20 20:02
P0003,голодный,199,10,
P0004,Űrállomás krízis,"999,5",1,2015.09.20 12:47
P0005,Ovális iroda,2 399,1,2015.09.20 07:31
'''

    def test_delimiter(self):
        self.writer = riwo.csv.Writer(self.resource, self.iterable_input, self.Schema, delimiter='\001')
        self.writer.write()
        self.content = u'''\
id\001name\001price\001quantity\001updated_at
P0001\001đói\001449\0011\0012015.09.20 20:00
P0002\001배고픈\001399\0011\0012015.09.20 20:02
P0003\001голодный\001199\00110\001
P0004\001Űrállomás krízis\001999,5\0011\0012015.09.20 12:47
P0005\001Ovális iroda\0012 399\0011\0012015.09.20 07:31
'''

    def test_quote(self):
        self.writer = riwo.csv.Writer(self.resource, self.iterable_input, self.Schema, \
                                      delimiter='\\', quotechar='`', quoting=riwo.csv.QUOTE_ALL)
        self.writer.write()
        self.content = u'''\
`id`\`name`\`price`\`quantity`\`updated_at`
`P0001`\`đói`\`449`\`1`\`2015.09.20 20:00`
`P0002`\`배고픈`\`399`\`1`\`2015.09.20 20:02`
`P0003`\`голодный`\`199`\`10`\``
`P0004`\`Űrállomás krízis`\`999,5`\`1`\`2015.09.20 12:47`
`P0005`\`Ovális iroda`\`2 399`\`1`\`2015.09.20 07:31`
'''

    def test_defined_schema_writer(self):
        self.writer = riwo.csv.Writer(self.resource, self.iterable_input, self.Schema)
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
        self.writer = riwo.csv.Writer(self.resource, self.Schema(self.iterable_input, mapper=dp.mapper.NAME))
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
            riwo.csv.Writer(self.resource, self.iterable_input)
        self.content = u''

    def test_unmarshal(self):
        self.writer = riwo.csv.Writer(self.resource, [], self.Schema)
        self.content = u''

        current_datetime = datetime.datetime.now()
        self.assertEqual(self.writer.unmarshal_item(current_datetime), current_datetime.isoformat())
        self.assertEqual(self.writer.unmarshal_item(4.21), u'4.21')
        self.assertEqual(self.writer.unmarshal_item(u'голодный'), u'голодный')

    def test_requisites(self):
        with self.assertRaises(riwo.exceptions.NestedSchemaNotSupported):
             riwo.csv.Writer(self.resource, [], self.NestedSchema)
        self.content = u''

    def tearDown(self):
        self.resource.close()
        with io.open(self.test_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertEqual(self.content, content)
        os.remove(self.test_file_path)
