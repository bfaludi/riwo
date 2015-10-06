# -*- coding: utf-8 -*-

from __future__ import absolute_import
import io
import os
import riwo
import daprot as dp
import datetime
import unittest
from riwo.compat import *

__dir__ = os.path.dirname(__file__)

class Groceries(dp.SchemaFlow):
    id = dp.Field('0:8')
    name = dp.Field('8:25', type=unicode, transforms=unicode.strip)
    price = dp.Field('25:34')
    quantity = dp.Field('34:40', type=long)
    updated_at = dp.Field('40:')

class TSV_Reader(object):
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

    def test_alma(self):
        print(list(self.reader))

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

class Test_TSV_Local_Reader_Schema(TSV_Reader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'source/test.fwt'), 'r', encoding='utf-8')
        self.reader = riwo.fwt.Reader(self.resource, Groceries)
