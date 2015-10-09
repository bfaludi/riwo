from __future__ import absolute_import
import daprot.mapper
import sqlalchemy.engine.base
import sqlalchemy.exc
from sqlalchemy import *
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter,
    exceptions
)

class Reader(AbstractReader):
    # void
    def __init__(self, connection, schema, offset=0, limit=None, db_schema=None, table=None, statement=None):
        self.db_schema = db_schema
        self.table = table
        self.statement = statement

        if not isinstance(connection, sqlalchemy.engine.base.Connection):
            raise AttributeError('`connection` has to be sqlalchemy.engine.base.Connection object in {self}.' \
                .format(self=self.name))

        self.metadata = MetaData()
        self.metadata.bind = connection

        if not self.table and not self.statement:
            raise AttributeError("`table` or `statement` attribute is required for {self}." \
                .format(self=self.name))

        if self.table and self.statement:
            raise AttributeError("Can't define `table` and `statement` at the same time for {self}." \
                .format(self=self.name))

        if self.table and not self.statement:
            table = Table(self.table, self.metadata, autoload=True, autoload_with=connection.engine, schema=self.db_schema)
            self.statement = table.select()

        super(Reader, self).__init__(connection, schema, offset, limit)

    # function
    def get_mapper(self):
        return daprot.mapper.NAME

    # Iterable
    def get_iterable_data(self):
        return ( dict(result_proxy_item) for result_proxy_item in self.resource.execute(self.statement) )

class Writer(AbstractWriter):
    # void
    def __init__(self, connection, iterable_data, schema=None, table=None):
        self.schema = schema
        self.table = table

        if not isinstance(connection, sqlalchemy.engine.base.Connection):
            raise AttributeError('`connection` has to be sqlalchemy.engine.base.Connection object in {self}.' \
                .format(self=self.name))

        self.metadata = MetaData()
        self.metadata.bind = connection

        super(Writer, self).__init__(connection, iterable_data, None)

    # void
    def prerequisite(self):
        pass

    # object
    def init_writer(self):
        if not self.table:
            return

        try:
            return Table(self.table, self.metadata, autoload=True, autoload_with=self.resource.engine, schema=self.schema)
        except sqlalchemy.exc.NoSuchTableError as e:
            pass

    # void
    def create(self, table, replace=False):
        if not isinstance(table, Table):
            raise AttributeError("{self}.create(table, replace=False) function's `table` attribute mush be Table object." \
                .format(self=self.name))

        # Set up missing variables.
        if not self.table: self.table = table.name
        if not self.schema: self.schema = table.schema

        # Recalibrate writer.
        self.writer = self.init_writer()

        # Remove table if replace is required.
        if self.writer is not None and replace:
            self.writer.drop(checkfirst=False)

        elif self.writer is not None:
            return self

        table.create(self.resource.engine)
        self.writer = self.init_writer()
        return self

    # void
    def truncate(self):
        if self.writer is None:
            return self

        self.writer.delete().execute()
        return self

    # void
    def write(self, buffer_size=None):
        if not buffer_size:
            # WARNING: It will contains the whole dataset in memory.
            self.resource.execute(self.writer.insert(), list(self.reader))
            return

        self.buffer = []
        self.buffer_size = int(buffer_size)
        super(Writer, self).write()
        if self.buffer:
            self.resource.execute(self.writer.insert(), self.buffer)

    # void
    def write_item(self, item):
        if len(self.buffer) >= self.buffer_size:
            self.resource.execute(self.writer.insert(), self.buffer)
            self.buffer = []

        self.buffer.append(item)
