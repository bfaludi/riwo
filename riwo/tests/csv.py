# -*- coding: utf-8 -*-

from __future__ import absolute_import
import io
import os
import riwo
import daprot as dp
import datetime
import unittest
import requests
import forcedtypes as t
from riwo.compat import *

__dir__ = os.path.join(os.path.dirname(__file__), 'source')
__remote__ = 'https://raw.githubusercontent.com/bfaludi/riwo/csv/riwo/tests/source'

class Groceries(dp.SchemaFlow):
    id = dp.Field(0)
    name = dp.Field(1, type=str, transforms=str.strip)
    price = dp.Field(2, type=t.new(t.Float, locale='hu_HU'))
    quantity = dp.Field(3, type=int)
    updated_at = dp.Field(4, type=t.Datetime, default_value=datetime.datetime.now)

class CommonCase(object):
    expected_result = [{
      u'quantity': 1,
      u'name': u'đói',
      u'updated_at': datetime.datetime(2015, 9, 20, 20, 0),
      u'price': 449.0,
      u'id': u'P0001'
    }, {
      u'quantity': 1,
      u'name': u'배고픈',
      u'updated_at': datetime.datetime(2015, 9, 20, 20, 2),
      u'price': 399.0,
      u'id': u'P0002'
    }, {
      u'quantity': 10,
      u'name': u'голодный',
      u'updated_at': datetime.datetime(2015, 9, 20, 12, 47),
      u'price': 199.0,
      u'id': u'P0003'
    }, {
      u'quantity': 1,
      u'name': u'Űrállomás krízis',
      u'updated_at': datetime.datetime(2015, 9, 20, 12, 47),
      u'price': 999.5,
      u'id': u'P0004'
    }, {
      u'quantity': 1,
      u'name': 'Ovális iroda',
      u'updated_at': datetime.datetime(2015, 9, 20, 7, 31),
      u'price': 2399.0,
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

class LocalReader(CommonCase, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test.csv'), 'r', encoding='utf-8')
        self.reader = riwo.csv.Reader(self.resource, Groceries)

class RequestsReader(CommonCase, unittest.TestCase):
    def setUp(self):
        self.resource = requests.get(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, Groceries)

class UrllibReader(CommonCase, unittest.TestCase):
    def setUp(self):
        self.resource = urlopen(os.path.join(__remote__, 'test.csv'))
        self.reader = riwo.csv.Reader(self.resource, Groceries)
