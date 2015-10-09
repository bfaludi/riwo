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

__remote__ = 'https://raw.githubusercontent.com/bfaludi/riwo/csv/riwo/tests/source'

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
        self.reader = riwo.csv.Reader(self.resource, IndexSchema)

class LocalHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-header.csv'), 'r', encoding='utf-8')
        self.reader = riwo.csv.Reader(self.resource, HeaderedSchema, use_header=True)

class RequestsReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema)

class RequestsHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'tes-headert.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema, use_header=True)

class UrllibReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema)

class UrllibHeaderReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test-header.csv'))
        self.reader = riwo.csv.Reader(self.resource, IndexSchema, use_header=True)
