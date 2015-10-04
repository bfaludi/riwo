# riwo (read input & write output)

**UNDER DEVELOPMENT!**

Please help me to finish this package as soon as humanly possible. Ping me in twitter [@bfaludi](http://twitter.com/bfaludi) if you want to help. :)

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

### 1. Load a fixed width text file and write out a JSON file.

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

## License

Copyright © 2015 Bence Faludi.

Distributed under the GPLv3 License.
