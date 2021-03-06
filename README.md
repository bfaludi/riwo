# riwo (read input & write output) [![Build Status](https://travis-ci.org/bfaludi/daprot.svg)](https://travis-ci.org/bfaludi/riwo)

**UNDER DEVELOPMENT!**

Please help me to finish this package as soon as humanly possible. Ping me in twitter ([@bfaludi](http://twitter.com/bfaludi)) if you want to help. :)

## Overview

This package is an universal Reader & Writer tool. Using the same interface in Python **2.7** & **3.3+**.

Important dependencies you should know about:

- [daprot](https://github.com/bfaludi/daprot) iterates over a dataset and load data into the right place.
- [dm](https://github.com/bfaludi/dm) maps the data to the right place within `daprot`.
- [funcomp](https://github.com/bfaludi/funcomp) carries out transformations on fields in `daprot`.

... and some nice package you should give it a try:

- [uniopen](https://github.com/bfaludi/uniopen) can open any kind of file/url/database with the same interface.
- [forcedtypes](https://github.com/bfaludi/forcedtypes) forces crappy quality data into python type.

**Give it a try because**:

- it works in Python **2.7** & **3.3+**.
- easy to use and quick!
- easy to extend. You can write your own extensions and it will be mapped automatically under `riwo`.
- it can handle `UTF-8` characters.

## Quick tutorial

### 1. Read a fixed width text file and write out a JSON file.

We have a fixed width text file and It contains product informations. (e.g.: id, name, formatted price in HUF, quantity, last updated date).

```
P0001   Tomato           449      1     2015.09.20 20:00
P0002   Paprika          399      1     2015.09.20 20:02
P0003      Cucumber      199      10
P0004   Chicken nuggets  999,5    1     2015.09.20 12:47
P0005   Chardonnay       2 399    1     2015.09.20 07:31
```

We want to convert it into JSON.

```python
import io
import riwo
import daprot as dp
import datetime
import forcedtypes as t

class Groceries(dp.SchemaFlow):
    id = dp.Field('0:8')
    name = dp.Field('8:25', type=str, transforms=str.strip)
    price = dp.Field('25:34', type=t.new(t.Float, locale='hu_HU'))
    quantity = dp.Field('34:40', type=int)
    updated_at = dp.Field('40:', type=t.Datetime, default_value=datetime.datetime.now)

with io.open('input.txt', 'r', encoding='utf-8') as inp, \
     io.open('output.json', 'w', encoding='utf-8') as out:

    reader = riwo.fwt.Reader(inp, Groceries)
    riwo.json.Writer(out, reader, pretty_print=True).write()
```

More examples are coming soon...

### 2. Move data between databases incrementally.

```python
import riwo
import daprot as dp
import uniopen
import forcedtypes as t
from sqlalchemy import *

def check_is_test_user(value):
    return value.endswith('@example.com')

class Tokens(dp.SchemaFlow):
    id = dp.Field('token_id')
    application_id = dp.Field('app_id')
    people_id = dp.Field('user_id', type=int)
    access_token = dp.Field(type=t.Str) # you want to convert '' to None.
    is_test = dp.Field('email', transforms=check_is_test_user)
    expire_date = dp.Field()
    created_at = dp.Field()

with uniopen.Open('postgresql://user:pass@localhost:5432/dbname') as db, \
     uniopen.Open('redshift://user:pass@host:port/dbname') as dwh:

    reader = riwo.sqlalchemy.Reader(db, Tokens, statement="""
        SELECT * FROM tokens 
        WHERE created_at::date >= CURRENT_DATE - interval '3 days';""")
    writer = riwo.sqlalchemy.Writer(dwh, reader) \
        .create(Table("tokens", MetaData(),
            Column('id', Integer, primary_key=True),
            Column('application_id', Integer),
            Column('people_id', Integer),
            Column('access_token', Unicode(255)),
            Column('is_test', Boolean),
            Column('expire_date', Date()),
            Column('created_at', Date()),
            schema="temp")) \
        .truncate() \
        .write(buffer_size=10000) # write 10k record at the same time 
    dwh.execute("""
        INSERT INTO public.tokens (
          SELECT * 
          FROM temp.tokens 
          WHERE id NOT IN (SELECT id FROM public.tokens)
        );""")
```

or if you don't want to do any magic in the `Reader` part, you can write it much shorter and It will still work.

```python
import riwo
import daprot as dp
import uniopen
from sqlalchemy import *

with uniopen.Open('postgresql://user:pass@localhost:5432/dbname') as db, \
     uniopen.Open('redshift://user:pass@host:port/dbname') as dwh:

    iterable = db.execute("""SELECT * FROM tokens;""")
    writer = riwo.sqlalchemy.Writer(dwh, iterable) \
        .create(Table("tokens", MetaData(),
            Column('id', Integer, primary_key=True),
            Column('application_id', Integer),
            Column('people_id', Integer),
            Column('access_token', Unicode(255)),
            Column('expire_date', Date()),
            Column('created_at', Date()),
            schema="temp")) \
        .truncate() \
        .write(buffer_size=10000) # write 10k record at the same time 
```

## License

Copyright © 2015 Bence Faludi.

Distributed under the GPLv3 License.
