# Working with folders

To load a dataset (path `a.b.c`) from Dremio use the the `get_dataset` method:

```python
dataset = dremio.get_dataset(['a','b','c'])
```

or

```python
dataset = dremio.get_dataset('a.b.c')
```

## Attributes

A folder has the following attributes:

- entityType: Literal["dataset"]
- id: UUID
- type: Literal["PHYSICAL_DATASET", "VIRTUAL_DATASET"]
- path: list[str]
- createdAt: datetime
- tag: str
- fields: list[Field]
- sql: Optional[str] = None # don't forget to commit after changing
- sqlContext: Optional[list[str]] = None
- owner: Optional[Owner] = None
- accelerationRefreshPolicy: Optional[DatasetAccelerationRefreshPolicy] = None
- format: Optional[DatasetFormat] = None
- approximateStatisticsAllowed: Optional[bool] = None
- accessControlList: Optional[AccessControlList] = None

## Properties

### `name`

To get the name of a VDS, use `Dataset.name`:

```python
dataset = dremio.get_dataset('a.b.c')
name = dataset.name
print(name)
```

A folders name is ALWAYS it's last path element!

## Methods

### `commit` and permissions

With `commit` you can upload changes from you local version of the dataset object to the online version:

```python
dataset = dremio.get_dataset('a.b.c')
dataset.sql = "SELECT * FROM a.b.d"
dataset.commit()
```

```python
dataset = dremio.get_dataset(['a','b','c'])
dataset.set_access_for_role("PUBLIC", ["SELECT"]) # set permissions for all users and roles
dataset.commit() # uploads the changes to the dremio instance
```

This is necessary to make changes to the dataset object.

### wiki

Get dataset wiki:

```python
wiki = dataset.get_wiki()
print(wiki)
```

To set the wiki text of a dataset:

```python
dataset = dremio.get_dataset('a.b.c')
wiki = dataset.get_wiki()
wiki.text = "This is a test wiki"
wiki.commit()
```

### `copy` and `reference`

`Dataset.copy` creates a copy of the the used VDS while using the same SQL statement:

```python
dataset = dremio.get_dataset(['a','b','c'])
dataset.copy(['a','b','d'])
```

Under the hood it uses [`Dremio.copy_dataset`](./DREMIO_METHODS.md#copy-a-vds)

Same works with `Dremio.reference` and [`Dremio.reference_dataset`](./DREMIO_METHODS.md#select--from-a-vds)

```python
dataset = dremio.get_dataset(['a','b','c'])
dataset.reference(['a','b','d'])
```

Both return the new dataset. (Mind this for chains)

### Chaining

```python
dataset = dremio.get_dataset('a.b.c') \
                .set_access_for_role("PUBLIC", ["SELECT"]) \
                .commit()
```

Harder to debug but faster to write...

### Reflections

#### List Reflection of a Dataset

To list all reflection of the dataset, just use `Dataset.reflections`

```pyhon
dataset = dremio.get_dataset(['a','b','c'])
reflections:list[Reflections] = dataset.reflections
```

#### Create Reflections

There are two ways to create reflections of a dataset:

1. create reflections recommended from dremio
2. create reflections manually

The simplest way is the recommended reflection method:

```python
dataset = dremio.get_dataset(['a','b','c'])
# to create reflections recommended from dremio (with optional param type="AGG"|"RAW"|"ALL", default "ALL")
dataset.create_recommended_reflections()
```

For more advanced users is the `create_reflection` method.
The `NewReflection` class follows the data model on [dremio/reflections](https://docs.dremio.com/current/reference/api/reflections/#creating-a-reflection).

```python
from dremio import NewReflection

dataset = dremio.get_dataset(['a','b','c'])
ref = NewReflection(...)
dataset.create_reflection(name="c_agg", ref)
```

#### Delete Reflection

For deletion of all reflections of a dataset, just use `Dataset.delete_reflections`:

```python
dataset = dremio.get_dataset(['a','b','c'])
dataset.delete_reflections()
```

To delete only one reflection, use the `Dremio.delete_reflection` method:

```python
dremio.delete_reflection(reflection_id)
```

Example: To delete only raw reflections of a dataset:

```python
dataset = dremio.get_dataset(['a','b','c'])
for ref in dataset.reflections:
  if ref.type == "RAW":
    dremio.delete_reflection(ref.id)
```

### `run`

With `Dataset.run` you can run a job on the dataset.
By default `run` will load the data from the dataset via arrow flight.
If the classic approche via http pagination is needed for some reason, it's possible to load the data with `Dataset.run(method='http')`.

Arguments:

- method: 'arrow' or 'http'. Default: 'arrow'

```python
dataset = dremio.get_dataset(['a','b','c'])
result = dataset.run()
```

This is also chainable:

```python
result = dremio.get_dataset(['a','b','c']).run()
```

See [`JobResult`](#jobresult) for more infos.

### `run_to_pandas` and `run_to_polars`

With `run_to_pandas` and `run_to_polars` you can load Datasets directly into polars or pandas dataframes.
It's a shortcut for `Dataset.run().to_polars()` or `Dataset.run().to_pandas()` that will run much faster because the result will not be converted to a python object occasionally.

```python
df = dremio.get_dataset(['a','b','c']).run_to_polars()
```

`df` is then a polars dataframe.

### `delete`

To delete a dataset use `Dataset.delete`:

```python
dataset = dremio.get_dataset(['a','b','c'])
dataset.delete()
```

# `JobResult`

To get a job result use ['Dataset.run'](#run).

The `JobResult` object as the following attributes:

- `rowCount: int` -> row number for all results
- `rows: list[dict[str,Any]]` -> contains all result lines
- `schema: list[Field]` -> gives the schema of the dataset

## Use the `JobResult` in python

There are a few properties to use the result

### `JobResult.dict`

Gives you a python dict out of the job result.

```python
result = dremio.get_dataset(['a','b','c']).run()
result_dict = result.dict
```

You get something like that:

```python
{
  'rowCount': 12, 
  'schema': [
    {
      'name': 'col',
      'type': {'name': 'VARCHAR'}
    }
  ],
  'rows': [
    {'col':'A'},
    {'col':'B'},
    {'col':'C'},
  ]
}
```

### `JobResult.json`

Run `result.json()` to get a json string:

```python
result = dremio.get_dataset(['a','b','c']).run()
with open("result.json","w") as file:
  file.write(result.json())
```

## `JobResult.to_polars` and `JobResult.to_pandas`

To explore the data behind a VDS there is the possibility to export a [job result](#jobresult) as [pandas](https://pandas.pydata.org/docs/getting_started/intro_tutorials/03_subset_data.html#how-do-i-select-specific-columns-from-a-dataframe) or [polars](https://docs.pola.rs/user-guide/getting-started/#installing-polars) Dataframe.

### Polars DataFrame

```python
dataset = dremio.get_dataset(['a','b','c'])
result = dataset.run()
dataframe = result.to_polars() # or result.to_pandas()
```

Now you can use all the cool polars features:

```python
dataframe = dremio.get_dataset(['a','b','c']) \
                  .run() \
                  .to_polars()

print(dataframe.median()) # to get a table with the medians for all columns with countables
```
