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
    id = dp.Field()
    name = dp.Field(type=unicode, transforms=unicode.strip)
    price = dp.Field()
    quantity = dp.Field(type=long)
    updated_at = dp.Field()

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

class LocalRootReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-root.json'), 'r', encoding='utf-8')
        self.reader = riwo.json.Reader(self.resource, Schema)

class LocalRouteReader(CommonReader, unittest.TestCase):
    def setUp(self):
        self.resource = io.open(os.path.join(__dir__, 'test-route.json'), 'r', encoding='utf-8')
        self.reader = riwo.json.Reader(self.resource, Schema, route='response/items')

