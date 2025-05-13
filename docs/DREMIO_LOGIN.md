# Dremio from enviroment variables

You can create a logged in Dremio instance from env with the static method `from_env()`:

```python
from dremio import Dremio

dremio = Dremio.from_env()
```

The default enviroment variables are:

- `DREMIO_HOSTNAME`
- `DREMIO_USERNAME`
- `DREMIO_PASSWORD`
- `DREMIO_MODUS`    # optional
- `DREMIO_PORT`     # optional
- `DREMIO_PROTOCOL` # optional

If needed, you can name them explicit as function parameters.

## `.env` file

To load the environment variables from an .env file, its recommended to use `load_env()`:

```python
from dremio import Dremio
from dotenv import load_dotenv

load_dotenv()
dremio = Dremio.from_env()
```

Maybe it is necessary to install `dotenv` first with:

```bash
pip install python-dotenv
```

# Dremio Auth

There are to ways to use the Dremio driver:

1. autherized instance
2. non-authorized instance

## Authorized Instance

In this mode all methods on the instance will be authorized with the given credentials. This is good for a bot or a script for example.

```python
from dremio import Dremio

dremio = Dremio(<hostname>,username=<username>,password=<password>)
-OR-
dremio = Dremio(<hostname>,token=<token>)

catalog = dremio.get_catalog(<uuid>)
```

## Unauthorized Usage

In this mode the unauthorized instance can be used by different users like for an API.

```python
from dremio import Dremio

dremio = Dremio(<hostname>)

catalog = dremio.auth(f'_dremio{<token>}').get_catalog(<uuid>)
```

## Combination

In this mode, for example, a technical user can be stored for basic functions, but a specific user session can still be used for individual functions.

```python
from dremio import Dremio

dremio = Dremio(<hostname>,username=<username>,password=<password>)

catalog_1 = dremio.get_catalog(<uuid>)
catalog_2 = dremio.auth(f'_dremio{<token>}').get_catalog(<uuid>)
```
