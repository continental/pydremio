# Some informations about types

## Pathes

If a method want a path to a folder or VDS there are two ways to write them:

1. as string: `'a.b.c'`
2. as list: `['a','b','c']`

It's recommended to use the list notation, because it prevents some issues with special characters.

```python
# recommended:
path = ['a','b','c']
```

If you want to use the string notation, use ALWAYS the single quote symbol `'` for string declaration. If there is any special character in the path, use `"..."` around the path element to prevent parsing errors:

```python
# invalid path:
path = 'path.to.a folder'

# valid path:
path = 'path.to."a folder"'
```

# Don't forget to login

For all this you need to [login your dremio connector instance](./DREMIO_LOGIN.md). I would recommend to use a `.env` file and this code:

```python
from dremio import Dremio

dremio = Dremio.from_env(load_dotenv=True)
```

or just use:

```python
from dremio import Dremio

dremio = Dremio(<hostname>,username=<username>,password=<password>)
```

# Query Dremio

To send a generic SQL to Dremio just use:

```python
result = dremio.query('SELECT * FROM ...')
```

To get a polars or pandas table:

```python
df = dremio.query('SELECT * FROM ...').to_polars()

# or 

df = dremio.query('SELECT * FROM ...').to_pandas()
```

`Dremio.query` uses arrow flight by default. This could lead to some issues, if this is not right configured.
Alternatively it is possible to query over http:

```python
result, job = dremio.query('SELECT * FROM ...', method='http')
df = result.to_polars()
```

> Attension!: `http` is not recommended for large tables and heavy data loading!! Use `http` only for Dremio SQL-Commands and smaller tables.
> Otherwise stay with `arrow`.

> For further information check out [JobResult](JOBRESULT.md)

# Get Dremio Objects

## `Dremio.get_dataset`

To get a fully types VDS from Dremio:

```python
dataset = dremio.get_dataset(['a','b','c'])
```

or

```python
dataset = dremio.get_dataset('a.b.c')
```

Now you can do all the cool things on `datasets`.
[Look here to learn more](./DATASET.md)

## `Dremio.get_folder`

To get a folder from dremio:

```python
folder = dremio.get_folder(['a','b','c'])
```

or

```python
folder = dremio.get_folder('a.b.c')
```

[Look at this for more information](./FOLDER.md)

# Create Dremio Objects

## Create a new [folder](./FOLDER.md)

```python
new_folder = dremio.create_folder('path.to.new.folder')
```

This will automatically create all folders on the path, if they don't exist.

For more information [watch this](./FOLDER.md)

## Copy a VDS

`Dremio.copy_dataset` creates a copy of a VDS and uses the same SQL statement:

```python
dremio.copy_dataset('path.to.source.vds','path.to.copy.vds')
```

## `SELECT * FROM` a VDS

`Dremio.reference_dataset` creates a reference of a VDS and uses a `SELECT * FROM path.to.source.vds`:

```python
dremio.reference_dataset('path.to.source.vds','path.to.copy.vds')
```

## Copy a folder

`Dremio.copy_folder` creates a copy of a folder with all folders and datasets inside.

```python
dremio.copy_folder('path.to.source.folder','path.to.copy.folder')
```

Since version 0.1.13 you can set the `relative_references` flag to `True` to re-reference all datasets that utilise datasets within the copied folder, directing them to the new copies. By default the flag is `False` because it changes sql statements - this shouldn't be done without knowing.

```python
dremio.copy_folder('path.to.source.folder','path.to.copy.folder',relative_references=True)
```

Optionally you can set the `assume_privileges` flag to `False` to remove all privileges from the copied folder. By default the flag is `True`.

## Reference a folder

`Dremio.reference_folder` creates a reference of a folder with all folders and datasets inside.

```python
dremio.reference_folder('path.to.source.folder','path.to.copy.folder')
```

## Move a folder (example)

```python
folder = dremio.get_folder('path.to.folder')
folder.copy('path.to.new.folder', relative_references=True)
dremio.delete_catalog_item(folder.id)
```

Or directly on the `Dremio` object:

```python
dremio.copy_folder('path.to.folder','path.to.new.folder',relative_references=True)
dremio.delete_catalog_item(dremio.get_folder('path.to.folder').id)
```

Currently there is no `move` method, but you can use the `copy` method with `relative_references` and delete the old folder.
We don't have a `move` method, because it should be a manual process to remove items to prevent data loss.
That is also the reason why there is no `delete` method on `datasets` and `folders`. You should always use the `delete_catalog_item` method on the `Dremio` object to delete items.

# Delete Dremio Objects

The only way to delete a dataset or folder is to use the `delete_catalog_item` method on the `Dremio` object.

```python
dremio.delete_catalog_item('id of the item')
```

You can get the id of an item by using the `id` attribute on a `dataset` or `folder` object.

```python
dataset = dremio.get_dataset('path.to.vds')
dremio.delete_catalog_item(dataset.id)
```

# Update Dremio Objects

[Look here for more information about dataset manipulation](./DATASET.md)

[Look here for more information about folder manipulation](./FOLDER.md)

# Data Fetching

The simplest way to get data out of Dremio is to query them with `Dremio.query()`.
If you need more control over this process, you can use the namespaces `flight` and `http`:

## Arrow Flight

The `flight` namespace provides a bunch of useful arrow flight related methods and objects:

- `Flight.client: pyarrow.FlightClient` (pre-configured)
- `Flight.query(<sql>) -> pyarrow.Table` 
- `Flight.query_dataset(<sql>) -> pyarrow.Table`
- `Flight.query_stream(<sql>) -> pyarrow.FlightStreamReader`

```python
from dremio import Dremio

dremio = Dremio(...)

dremio.flight.query('SELECT * FROM A.B.C')
```