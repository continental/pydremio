
# Dremio Driver

## Getting Started

### Latest Version

To use this dremio connector simply run:

```bash
pip install --upgrade --force-reinstall https://github.com/continental/pydremio/releases/download/v0.3.1/dremio-0.3.1-py3-none-any.whl
```

> For older Python versions (<3.11) use the install of the current version via [specific verion](#specific-version)

or 

add the link as line to your `requirements.txt`:

```txt
...
python-dotenv == 1.0.1
https://github.com/continental/pydremio/releases/latest/download/dremio-latest-py3-none-any.whl
...
```

>❗️ VPN connection or be on premise needed!
>To install the dremio connector you have to be inside the Continental network.

<details>
  <summary>If this doesn't work ...</summary>

  on Windows:

  ```bash
  py -m pip install --upgrade --force-reinstall https://github.com/continental/pydremio/releases/latest/download/dremio-latest-py3-none-any.whl
  ```

  Mac/Linux:

  ```bash
  python3 -m pip install --upgrade --force-reinstall https://github.com/continental/pydremio/releases/latest/download/dremio-latest-py3-none-any.whl
  ```

</details>

### Specific Version

To use a specific version of the dremio connector just modify the import link in the following schema:

```
pip install https://github.com/continental/pydremio/releases/download/v<version>/dremio-<version>-py3-none-any.whl
```

For the current version `v0.3.1` it would look like this:

```
pip install --force-reinstall https://github.com/continental/pydremio/releases/download/v0.3.1/dremio-0.3.1-py3-none-any.whl
```

## Basic usage

### login

The simplest way to use the dremio connector is to create a logged in instance:

```python
from dremio import Dremio

dremio = Dremio(<hostname>,username=<username>,password=<password>)
```

Or just use the environment variables or an `.env` file:

```python
from dremio import Dremio
from dotenv import load_dotenv

load_dotenv()
dremio = Dremio.from_env()
```

More infos here: [Dremio Auth](docs/DREMIO_LOGIN.md)

## Examples

### Load a Dataset

```python
from dremio import Dremio

dremio = Dremio(
  hostname=<hostname>,
  username=<username>,
  password=<password>
)

ds = dremio.get_dataset("path.to.vds")
polars_dataframe = ds.run().to_polars()
pandas_dataframe = ds.run().to_pandas()
```

### Create folder

```python
from dremio import Dremio, NewFolder

folder = NewFolder(['<path...>','<...to folder>','<folder name>'])
dremio.create_catalog_item(folder)
```

Create a folder with access control:

```python
from dremio import Dremio, NewFolder, AccessControlList, AccessControl

ac = AccessControlList(users = [AccessControl('<user id>',['SELECT'])])

folder = NewFolder(['<path...>','<...to folder>','<folder name>'])
folder.accessControlList = ac
dremio.create_catalog_item(folder)
```

## Methods

All models can be found in ./models/

### Connection

- login(username:str && password:str) -> token:str
- auth(auth:str=None || token:str=None) -> new Dremio instance

### Catalog

- get_catalog_by_id(id:UUID) -> CatalogObject
- get_catalog_by_path(path:list[str]) -> CatalogObject
  - path should be a list like: ["space1","weather"]
  but its possible to write the path like this: "space1/weather"
- create_catalog_item(item:NewCatalogObject|dict) -> CatalogObject
- update_catalog_item(id:UUID || item:NewCatalogObject|dict) -> CatalogObject
- update_catalog_item_by_path(path:list[str] && item:NewCatalogObject|dict) -> CatalogObject
- delete_catalog_item(id:UUID) -> bool
  - returns true if the deletion was successfull
- copy_catalog_item_by_path(path:list[str], new_path:list[str]) -> CatalogObject
- refresh_catalog(id:UUID) -> CatalogObject
- get_catalog_tree(id:str=None, path:str|list[str]=None)
  - this will give a full tree of all objects in the catalog, but be careful, this is a very expensive function an ist only for exploration and mapping
- get_dataset(path:list[str]|str|None = None, *, id:UUID|None=None) -> Dataset
- create_dataset(path:list[str]|str, sql:str|SQLRequest, type:Literal['PHYSICAL_DATASET', 'VIRTUAL_DATASET']='VIRTUAL_DATASET') -> Dataset
- delete_dataset(path:list[str]|str) -> bool
- get_folder(path:list[str]|str|None = None, *, id:UUID|None=None) -> Folder
- create_folder(path: str|list[str]) -> Folder
- delete_folder(path: str|list[str], recursive:bool=True) -> bool
- copy_dataset(source_path:list[str]|str, target_path:list[str]|str) -> Dataset
- reference_dataset(source_path:list[str]|str, target_path:list[str]|str) -> Dataset
- copy_folder(source_path:list[str]|str, target_path:list[str]|str, *, assume_privileges:bool=True, relative_references:bool=False) -> Folder
- reference_folder(source_path:list[str]|str, target_path:list[str]|str, *, assume_privileges:bool=True) -> Folder


### Collaboration

Wiki and Tags by id of collection item:
The tags-object contains an array og tags.

- get_wiki(id:UUID) -> Wiki
- set_wiki(id:UUID, wiki:Wiki) -> Wiki:
- get_tags(id:str) -> Tags
- set_tags(id:str, tags:Tags) -> Tags

### SQL

- sql(sql_request:SQLRequest) -> JobId
- start_job_on_dataset(id:UUID) -> JobId
- get_job_info(id:UUID) -> Job
- cancel_job(id:UUID) -> Job
- get_job_results(id:UUID) -> JobResult
- sql_results(sql_request:SQLRequest) -> Job|JobResult

### User

- get_users() -> list[User]
- get_user(id:UUID) -> User
- get_user_by_name(name:str) -> User
- create_user(user:User) -> User
- update_user(id:UUID, user:User) -> User
- delete_user(id:UUID, tag:str) -> boo
  - returns true if the deletion was successful
