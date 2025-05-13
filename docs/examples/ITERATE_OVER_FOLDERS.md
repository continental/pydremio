
# Folder iteration

Sometimes it can be useful to iterate over a folder to get some information or to automate a dremio task.
For this cases there is an iterator for folder objects.
To use it, just load the folder and use it like a list:

```python
from dremio import Dremio

dremio = Dremio.from_env(load_dotenv=True)

folder = dremio.get_folder(['a','b','c'])

for child in folder:
  # do some stuff
```

All child will be [`CatalogObject`s](./../CATALOG_OBJECTS.md)

