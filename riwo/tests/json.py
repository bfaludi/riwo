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

