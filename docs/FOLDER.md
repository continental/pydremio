# Working with folders

To load a folder (path `a.b.c`) from Dremio use the the `get_folder` method:

```python
folder = dremio.get_folder(['a','b','c'])
```


## Attributes

A folder has the following attributes:
- entityType: Literal["folder"]
- id: UUID
- path: list[str]
- tag: str
- children: Optional[list[CatalogElement]] = None
- accessControlList: Optional[AccessControlList] = None
- permissions: Optional[list[Privilege]] = None
- owner: Optional[Owner] = None

## Properties

### `name`

To get the name of a folder, use `Folder.name`:

```python
folder = dremio.get_folder('a.b.c')
name = folder.name
print(name)
```

A folders name is ALWAYS it's last path element!

## Methods

### `commit` and permissions

With `commit` you can upload changes from you local version of the folder object to the online version:

```python
folder = dremio.get_folder(['a','b','c'])
folder.set_access_for_role("PUBLIC", ["SELECT"]) # set permissions for all users and roles
folder.commit() # uploads the changes to the dremio instance
```

### `copy` and `reference`

See [examples](#examples)

### Chaining

```python
folder =  dremio.get_folder('a.b.c') \
                .set_access_for_role("PUBLIC", ["SELECT"]) \
                .commit()
```

Harder to debug but faster to write...

### iterator

To iterate over its children use the python `for` loop:

```python
for child in folder:
    # do something with the 
```

The children are `CatalogElement`.

## Examples

### Copy a folder

```python
folder = dremio.get_folder(['a','b','c'])
new_folder = folder.copy(['a','b','d'])
```

The different between both methods is that `copy` will let the contained VDSs point on the same sources like the original, while `reference` will point them ON the original.

To change all references to the original VDSs in the copied folder to the new VDSs you can set the `relative_references` flag to `True`:

```python
new_folder = folder.copy(['a','b','d'], relative_references=True)
```

You can also use the related method on `Dremio`:

```python
new_folder = dremio.copy_folder(['a','b','c'], ['a','b','d'])
```

[Look here for more information](./DREMIO_METHODS.md#copy-a-folder)

It will do the same like the prev example.

### Reference a folder

```python
folder = dremio.get_folder(['a','b','c'])
new_folder = folder.reference(['a','b','d'])
```

The different between both methods is that `copy` will let the contained VDSs point on the same sources like the original, while `reference` will point them ON the original.

### Conditional loop over folder children

If you want to loop over all children of a folder and only do something with the VDSs you can use the following code:

```python
folder = dremio.get_folder(['a','b','c'])
for child in folder:
    # to get only the VDSs
    if child.datasetType:
        # do something with the VDS

    # if the child is folder the loop should
    if child.containerType:
        # do something with the folder
```

For better programming style it is recommended to use the `continue` statement:

```python
folder = dremio.get_folder(['a','b','c'])

for child in folder:
    # if the child is folder the loop should continue
    if not child.datasetType:
        continue

    # do something with the VDS
```

```python
folder = dremio.get_folder(['a','b','c'])

for child in folder:
    # if the child is folder the loop should continue
    if not child.datasetType:
        continue

    # do something with the VDS
```

### Reference all VDSs in a folder without nested folders

```python
import logging
from dremio import Dremio, DremioConnectorError

dremio = Dremio.from_env(load_dotenv=True)

folder = dremio.get_folder(['a','b','c'])

target_folder_path = dremio.create_folder(['e','f','g']).path

for child in folder:
  # if the child is folder the loop should continue
  if not child.datasetType:
    continue

  # get the source dataset name
  name = child.path[-1]

  # create the target path for the new VDS: 
  # ['e','f','g',name] = ['e','f','g'] + [name]
  reference_path = target_folder_path + [name]

  # try to create a `SELECT * FROM child` reference
  try:
    dremio.reference_catalog_item_by_path(child.path, reference_path)
  except DremioConnectorError as e:
    # if its fails it should give you the error message as warning
    logging.warn(e.errorMessage)
```
