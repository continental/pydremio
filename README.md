# pydremio

## Introduction

*pydremio* is an API wrapper to interact with *Dremio*. It can be used to perform operations on datasets available in *Dremio* or to perform operations on meta data of these datasets. Interaction can be done via http (API) or via *Arrow Flight*. Since *Arrow Flight* is much more performant it should be used for operations on data.

This repository includes the code for the wrapper itself, unit tests and examples to get started. The wrapper is distributed as wheel and can be found in the release-section. It is also planned to provide the wheel via *PyPi* in the near future.

## Getting Started

- The latest version of *pydremio* can be installed by (*Python* v3.13 required):

```bash
pip install --upgrade --force-reinstall https://github.com/continental/pydremio/releases/download/v0.3.1/dremio-0.3.1-py3-none-any.whl
```

or via `requirements.txt`

```txt
python-dotenv == 1.0.1
https://github.com/continental/pydremio/releases/latest/download/dremio-latest-py3-none-any.whl
```

- Other versions can be installed by:

```
pip install https://github.com/continental/pydremio/releases/download/<version>/dremio-<version>-py3-none-any.whl
```

## Basic usage

### Login

The simplest way to use the *pydremio* is to create a logged in instance:

```python
from dremio import Dremio

dremio = Dremio(<hostname>,username=<username>,password=<password>)
```

Replace hostname, username and password (or PAT) with valid values or use environment variables (e.g., via `.env`-file). We highly recommend to NOT store your credentials in the code itself and use the env-approach instead!

The env-file should look like this:

```txt
DREMIO_USERNAME = "your_username@example.com"
DREMIO_PASSWORD = "xyz-your-password-or-pat-xyz"
DREMIO_HOSTNAME = "https://your.dremio.host.cloud"
```

There is a function `from_env()` which does the job of loading for you:

```python
from dremio import Dremio
from dotenv import load_dotenv

load_dotenv()
dremio = Dremio.from_env()
```

More infos can be found here: [Dremio authentication](docs/DREMIO_LOGIN.md)

## Examples

### Load a dataset

```python
from dremio import Dremio

dremio = Dremio.from_env()

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

### Create a folder (with access control):

```python
from dremio import Dremio, NewFolder, AccessControlList, AccessControl

ac = AccessControlList(users = [AccessControl('<user id>',['SELECT'])])

folder = NewFolder(['<path...>','<...to folder>','<folder name>'])
folder.accessControlList = ac
dremio.create_catalog_item(folder)
```

## Methods

All models can be found in [./models/](./models/). Here is a list of all available methods sorted by category:

### Connection

- login(username:str && password:str) -> token:str
- auth(auth:str=None || token:str=None) -> new Dremio instance

## 📚 Catalog

### Retrieval
- `get_catalog_by_id(id: UUID) -> CatalogObject`
- `get_catalog_by_path(path: list[str]) -> CatalogObject`  
  - `path` should be a list like `["space1", "weather"]`, but strings like `"space1/weather"` are also accepted.

### Creation
- `create_catalog_item(item: NewCatalogObject | dict) -> CatalogObject`

### Updating
- `update_catalog_item(id: UUID | item: NewCatalogObject | dict) -> CatalogObject`
- `update_catalog_item_by_path(path: list[str], item: NewCatalogObject | dict) -> CatalogObject`

### Deletion
- `delete_catalog_item(id: UUID) -> bool`  
  - Returns `True` if deletion was successful.

### Copying
- `copy_catalog_item_by_path(path: list[str], new_path: list[str]) -> CatalogObject`

### Refreshing
- `refresh_catalog(id: UUID) -> CatalogObject`

### Exploration
- `get_catalog_tree(id: str = None, path: str | list[str] = None)`  
  - Returns the full tree of catalog objects.  
  - ⚠️ **Expensive** operation, intended for exploration and mapping only.

## 📊 Dataset

- `get_dataset(path: list[str] | str | None = None, *, id: UUID | None = None) -> Dataset`
- `create_dataset(path: list[str] | str, sql: str | SQLRequest, type: Literal['PHYSICAL_DATASET', 'VIRTUAL_DATASET'] = 'VIRTUAL_DATASET') -> Dataset`
- `delete_dataset(path: list[str] | str) -> bool`
- `copy_dataset(source_path: list[str] | str, target_path: list[str] | str) -> Dataset`
- `reference_dataset(source_path: list[str] | str, target_path: list[str] | str) -> Dataset`

## 🗂️ Folder

- `get_folder(path: list[str] | str | None = None, *, id: UUID | None = None) -> Folder`
- `create_folder(path: str | list[str]) -> Folder`
- `delete_folder(path: str | list[str], recursive: bool = True) -> bool`
- `copy_folder(source_path: list[str] | str, target_path: list[str] | str, *, assume_privileges: bool = True, relative_references: bool = False) -> Folder`
- `reference_folder(source_path: list[str] | str, target_path: list[str] | str, *, assume_privileges: bool = True) -> Folder`

## 🤝 Collaboration

Wiki and tags are associated by the **ID of the collection item**.  
The tags object contains an array of tags.

- `get_wiki(id: UUID) -> Wiki`
- `set_wiki(id: UUID, wiki: Wiki) -> Wiki`
- `get_tags(id: str) -> Tags`
- `set_tags(id: str, tags: Tags) -> Tags`

## 🧠 SQL

- `sql(sql_request: SQLRequest) -> JobId`
- `start_job_on_dataset(id: UUID) -> JobId`
- `get_job_info(id: UUID) -> Job`
- `cancel_job(id: UUID) -> Job`
- `get_job_results(id: UUID) -> JobResult`
- `sql_results(sql_request: SQLRequest) -> Job | JobResult`

## 👤 User

- `get_users() -> list[User]`
- `get_user(id: UUID) -> User`
- `get_user_by_name(name: str) -> User`
- `create_user(user: User) -> User`
- `update_user(id: UUID, user: User) -> User`
- `delete_user(id: UUID, tag: str) -> bool`  
  - Returns `True` if deletion was successful.
