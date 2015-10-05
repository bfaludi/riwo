from __future__ import absolute_import
import daprot.mapper
import sqlalchemy.engine.base
from sqlalchemy import *
from .. import (
    Reader as AbstractReader,
    Writer as AbstractWriter
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
