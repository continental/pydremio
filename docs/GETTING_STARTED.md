# Getting Started

## Installation

To install this connector use:

> This is a prerelease!

```bash
pip install https://github.com/continental/pydremio/releases/download/v0.3.2/dremio-0.3.2-py3-none-any.whl
```

Or you can use it inside a virtual environment, `pipenv` for example:
```bash
pipenv install https://github.com/continental/pydremio/releases/download/v0.3.2/dremio-0.3.2-py3-none-any.whl
```

Import the connector with:
```python
from dremio import Dremio
```

Now you can connect a dremio instance:

```python
dremio = Dremio('<your hostname>', username='<your username>', password='<your password / token>')
## OR ##
dremio = Dremio('<your hostname>', token='<your token>')
```

See [login](DREMIO_LOGIN.md) for more information...

## Configuration

If your Dremio instance uses not the default settings, you can configure it by:

```python
# other port then 443
dremio = Dremio('<your hostname>', ..., port=8000)

# http instead of https
dremio = Dremio('<your hostname>', ..., protocol="http")

# arrow flight config
dremio = Dremio('<your hostname>', ..., flight_config={"port": 32010, "tls": True})
```

### Arrow Flight

This are the default arrow flight settings:

```python
port: int = 32010
tls: bool = False
disable_certificate_verification: bool = False
allow_autoconfig: bool = True
path_to_certs: str # auto get from `certifi.where()`
```

If `allow_autoconfig` is set to `True`, the connector will try to auto-config tls and cert settings. Set it to `False` if you don't want this behavior.
