# pydremio

## Introduction

**pydremio** is a Python API wrapper for interacting with [Dremio](https://www.dremio.com/).  
It allows you to perform operations on datasets and metadata within Dremio via either the **HTTP API** or **Arrow Flight**.  
Since *Arrow Flight* offers significantly better performance, it is the recommended method for data operations.

This repository includes the core library, unit tests, and example code to help you get started.

The wrapper is distributed as a Python wheel (`.whl`) and can be found in the [Releases](https://github.com/continental/pydremio/releases) section.  
Published to [PyPI](https://pypi.org/project/pydremio/).

## Installation

You need Python **3.13** or higher.

### Option 1: Install via pip

```bash
pip install pydremio
```

### Option 2a: Install via pip from GitHub

```bash
pip install --upgrade --force-reinstall https://github.com/continental/pydremio/releases/download/v0.3.2/dremio-0.3.2-py3-none-any.whl
```

If you are behind a **corporate firewall** and you need a **workaround** (NOT recommended for use in production!):

```bash
pip install --upgrade --force-reinstall \
  --trusted-host pypi.org \
  --trusted-host files.pythonhosted.org \
  --trusted-host github.com \
  --trusted-host objects.githubusercontent.com \
  --cert False \
  https://github.com/continental/pydremio/releases/download/v0.3.2/dremio-0.3.2-py3-none-any.whl
```

#### Install a specific version

```bash
pip install https://github.com/continental/pydremio/releases/download/<version>/dremio-<version>-py3-none-any.whl
```

### Option 2b: Use `requirements.txt`

```txt
python-dotenv == 1.0.1
https://github.com/continental/pydremio/releases/latest/download/dremio-latest-py3-none-any.whl
```

## Getting Started

### Logging in

The simplest way to create a logged-in client instance:

```python
from dremio import Dremio

dremio = Dremio(<hostname>, username=<username>, password=<password>)
```

Replace the placeholders or, preferably, use environment variables (via a `.env` file) to avoid storing credentials in code.

**Example `.env` file:**

```txt
DREMIO_USERNAME="your_username@example.com"
DREMIO_PASSWORD="xyz-your-password-or-pat-xyz"
DREMIO_HOSTNAME="https://your.dremio.host.cloud"
```

You can then use the convenience method:

```python
from dremio import Dremio
from dotenv import load_dotenv

load_dotenv()
dremio = Dremio.from_env()
```
By default pydremio assumes no TLS encryption. If you have set up TLS please use:

```python
from dremio import Dremio
from dotenv import load_dotenv

load_dotenv()
dremio = Dremio.from_env()

dremio.flight_config.tls = True
```

or set it up in your `.env`-file:

```txt
DREMIO_FLIGHT_TLS=TRUE
```

More information here: [Dremio authentication](docs/DREMIO_LOGIN.md)

## Examples

- By default, the queries are run with *Arrow Flight*.
- The reason behind is that http-queries generate a lot of temporary cache. This cache is stored for longer time and for each query again. This may cause high storage-costs if you query big tables!
- For small datasets this may not a good trade-off in duration. Try `run(method='http')` instead.

### Load a dataset

```python
from dremio import Dremio

dremio = Dremio.from_env()

ds = dremio.get_dataset("path.to.vds")
polars_df = ds.run().to_polars()
pandas_df = ds.run().to_pandas()
```

### Create a folder

```python
from dremio import Dremio, NewFolder

folder = dremio.create_folder("path.to.folder")
```

### Create a folder with access control

```python
from dremio import Dremio, NewFolder, AccessControlList, AccessControl

folder = dremio.create_folder("path.to.folder")
user_id = dremio.get_user_by_name('<user_name>')
folder.set_access_for_user(user_id, ['SELECT'])
```

## Methods

All models are located in the [`models/`](src/dremio/models/) directory.  
Below is an overview of available methods grouped by category.

### 🔐 Connection

- `login(username: str, password: str) -> str`
- `auth(auth: str = None, token: str = None) -> Dremio`

### 📚 Catalog

#### Retrieval
- `get_catalog_by_id(id: UUID) -> CatalogObject`
- `get_catalog_by_path(path: list[str]) -> CatalogObject`  
  - Accepts both list format (`["space", "dataset"]`) and string format (`"space/dataset"`)

#### Creation
- `create_catalog_item(item: NewCatalogObject | dict) -> CatalogObject`

#### Updating
- `update_catalog_item(id: UUID | item: NewCatalogObject | dict) -> CatalogObject`
- `update_catalog_item_by_path(path: list[str], item: NewCatalogObject | dict) -> CatalogObject`

#### Deletion
- `delete_catalog_item(id: UUID) -> bool`  
  - Returns `True` if successful

#### Copying
- `copy_catalog_item_by_path(path: list[str], new_path: list[str]) -> CatalogObject`

#### Refreshing
- `refresh_catalog(id: UUID) -> CatalogObject`

#### Exploration
- `get_catalog_tree(id: str = None, path: str | list[str] = None)`  
  - ⚠️ Expensive operation, intended for exploration and mapping only

### 📊 Dataset

- `get_dataset(path: list[str] | str | None = None, *, id: UUID | None = None) -> Dataset`
- `create_dataset(path: list[str] | str, sql: str | SQLRequest, type: Literal['PHYSICAL_DATASET', 'VIRTUAL_DATASET'] = 'VIRTUAL_DATASET') -> Dataset`
- `delete_dataset(path: list[str] | str) -> bool`
- `copy_dataset(source_path: list[str] | str, target_path: list[str] | str) -> Dataset`
- `reference_dataset(source_path: list[str] | str, target_path: list[str] | str) -> Dataset`

### 🗂️ Folder

- `get_folder(path: list[str] | str | None = None, *, id: UUID | None = None) -> Folder`
- `create_folder(path: str | list[str]) -> Folder`
- `delete_folder(path: str | list[str], recursive: bool = True) -> bool`
- `copy_folder(source_path: list[str] | str, target_path: list[str] | str, *, assume_privileges: bool = True, relative_references: bool = False) -> Folder`
- `reference_folder(source_path: list[str] | str, target_path: list[str] | str, *, assume_privileges: bool = True) -> Folder`

### 🤝 Collaboration

Wiki and tags are associated by the **ID of the collection item**.  
The tags object contains an array of tags.

- `get_wiki(id: UUID) -> Wiki`
- `set_wiki(id: UUID, wiki: Wiki) -> Wiki`
- `get_tags(id: str) -> Tags`
- `set_tags(id: str, tags: Tags) -> Tags`

### 🧠 SQL

- `sql(sql_request: SQLRequest) -> JobId`
- `start_job_on_dataset(id: UUID) -> JobId`
- `get_job_info(id: UUID) -> Job`
- `cancel_job(id: UUID) -> Job`
- `get_job_results(id: UUID) -> JobResult`
- `sql_results(sql_request: SQLRequest) -> Job | JobResult`

### 👤 User

- `get_users() -> list[User]`
- `get_user(id: UUID) -> User`
- `get_user_by_name(name: str) -> User`
- `create_user(user: User) -> User`
- `update_user(id: UUID, user: User) -> User`
- `delete_user(id: UUID, tag: str) -> bool`  
  - Returns `True` if deletion was successful

## Roadmap

- [x] Publish to PyPI
- [ ] CLI support
<!-- - [ ] Async support -->

## Contributing

Contributions are welcome! Please open issues or pull requests for features, bugs, or improvements.

## License

This project is licensed under the BSD License. See the [LICENSE](LICENSE.txt) file for details.
