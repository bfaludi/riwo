import imp
import pip
import daprot as dp
from . import exceptions

class RiWo(object):
    # str
    @property
    def name(self):
        return '{}.{}'.format(self.__class__.__module__, self.__class__.__name__)

    # str
    @property
    def encoding(self):
        if hasattr(self.resource, 'encoding'):
            return str(self.resource.encoding)

        return 'utf-8'

class Reader(RiWo):
    # void
    def __init__(self, resource, schema, offset=0, limit=None):
        self.resource = resource
        self.schema = schema
        self.reader = self.init_reader()
        self.offset = offset
        self.limit = limit

    # dp.SchemaFlow
    def init_reader(self):
        return self.schema(self.get_iterable_data(), mapper=self.get_mapper())

    # function
    def get_mapper(self):
        return dp.mapper.IGNORE

    # Iterable
    def get_iterable_data(self):
        raise RuntimeError("{self}'s `get_iterable_data()` function is not implemented yet!" \
            .format(self=self.name))

    # tuple
    def offset():
        # int
        def fget(self):
            return self.reader.offset

        # void
        def fset(self, value):
            self.reader.offset = int(value or 0)

        # void
        def fdel(self):
            self.reader.offset = 0

        return locals()
    offset = property(**offset())

    # tuple
    def limit():
        # int
        def fget(self):
            return self.reader.limit

        # void
        def fset(self, value):
            if value is None:
                del self.limit
                return
            self.reader.limit = int(value)

        # void
        def fdel(self):
            self.reader.limit = None

        return locals()
    limit = property(**limit())

    # list<str>
    @property
    def fieldnames(self):
        return self.reader.fieldnames

    # bool
    def is_nested(self):
        return self.reader.is_nested()

    # Reader
    def __iter__(self):
        return self

    # type
    def next(self):
        return next(self.reader)

    # type
    __next__ = next

class Writer(RiWo):
    # void
    def __init__(self, resource, iterable_data, input_schema=None):
        self.resource = resource
        self.iterable_data = iterable_data
        self.input_schema = input_schema

        self.prerequisite()

        self.reader = self.init_reader()
        self.writer = self.init_writer()

    # void
    def prerequisite(self):
        if not self.current_schema:
            raise exceptions.SchemaRequired('No schema was defined for {self}' \
                .format(self=self.name))

        elif dp.SchemaFlow not in self.current_schema.__bases__:
            raise exceptions.SchemaRequired('Schema has to be dp.SchemaFlow at {self}' \
                .format(self=self.name))

    # dp.SchemaFlow
    @property
    def current_schema(self):
        if isinstance(self.iterable_data, dp.SchemaFlow):
            return self.iterable_data.__class__
        elif isinstance(self.iterable_data, Reader):
            return self.iterable_data.schema
        elif self.input_schema is not None:
            return self.input_schema

        return None

    # dp.SchemaFlow
    def init_reader(self):
        return self.iterable_data

    # object
    def init_writer(self):
        pass

    # dp.SchemaFlow
    @property
    def flow_reader(self):
        if not isinstance(self.reader, (dp.SchemaFlow, Reader)):
            return self.current_schema([])

        return self.reader

    # list<str>
    @property
    def fieldnames(self):
        if hasattr(self, '_fieldnames'):
            return self._fieldnames

        self._fieldnames = self.flow_reader.fieldnames
        return self._fieldnames

    # bool
    def is_nested(self):
        return self.flow_reader.is_nested()

    # list<dict>
    def read_items(self):
        return list(self.reader)

    # void
    def write(self):
        for item in self.reader:
            self.write_item(item)

    # void
    def write_item(self, item):
        raise extension.NotImplemented("{self}'s `write_item(item)` function is not implemented yet!" \
            .format(self=self.name))

from .dialects import *

# Load all extension
for package in filter(lambda x: x.project_name.startswith('riwo-'), pip.get_installed_distributions()):
    extension_name = package.project_name.split('-',1)[-1]
    f, filename, description = imp.find_module(package.project_name)
    loaded_package = imp.load_module(package.project_name, f, filename, description)
    exec('{package} = loaded_package'.format(package=extension_name))
