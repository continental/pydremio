# Using Jobs and JobResults

## Ways to get a JobResult

JobResults are a general abstraction of a table.
There are several ways to get one: 

- run a [dataset](DATASET.md) with `Dataset.run()`.
- run a [query](DREMIO_METHODS.md#query-dremio) with `Dremio.query`.
- run a lower level query like `Dremio._flight_query()` or `Dremio._http_query()`.

## Deal with JobResults

```python
res = dremio.get_dataset('A.B.C').run()

## OR ##

res = dremio.query('SELECT * FROM A.B.C')

type(res) # JobResult
```

### `.dict` generates a python dict

To get a simple python dict, just use this property:

```python
d = res.dict
```

### Schema information

With `.schema`, `.schema_names` and `.schema_types` it is possible to get more information about the result schema:

```python
print(res.schema_names)
```

Gives you `['name', 'country', 'subcountry']` for example.

### Prints

To get an overview of the `JobResult` just print it:

```python
print(res)
```

```bash

len: 26467      cols: 4

+---------------------+----------------------+-----------------------+-----------+
| name                | country              | subcountry            | geonameid |
+---------------------+----------------------+-----------------------+-----------+
| string              | string               | string                | int64     |
+---------------------+----------------------+-----------------------+-----------+
| les Escaldes        | Andorra              | Escaldes-Engordany    | 3040051   |
| Andorra la Vella    | Andorra              | Andorra la Vella      | 3041563   |
| Umm Al Quwain City  | United Arab Emirates | Imārat Umm al Qaywayn | 290594    |
| Ras Al Khaimah City | United Arab Emirates | Raʼs al Khaymah       | 291074    |
| Zayed City          | United Arab Emirates | Abu Dhabi             | 291580    |
| ...                 | ...                  | ...                   | ...       |
+---------------------+----------------------+-----------------------+-----------+

hint: run `.to_polars()` or `.to_pandas()` for more options

```

### Explore some data

It's possible to get some basic data processing like 'get a column' or 'get a row':

```python
# get the first row
first_row = res[0]

# get the column 'name'
names = res['name']

# get the first item of column `name`
first_name = res[0]["name"] # or res["name"][0]
```

### Pandas, Polars, PyArrow Tables

It is recommended that you use one of the dataframe options to speed up your work with the results:

```python
df = res.to_polars()
## OR ##
df = res.to_pandas()
```