__all__ = ["Dremio"]  # this is like `export ...` in typescript

import logging
from urllib import parse
import requests
from requests.exceptions import JSONDecodeError


from ..utils.validation import Validation
from ..utils.converter import path_to_list
from ..models import *


class Dremio:

    v = Validation()

    def __init__(
        self,
        hostname: str,
        *,
        username: Union[str, None] = None,
        password: Union[str, None] = None,
        auth: Union[str, None] = None,
        token: Union[str, None] = None,
        modus: Literal["stand_alone", "server"] = "stand_alone",
        port: Union[int, None] = None,
        protocol: Union[Literal["https", "http"], None] = None,
        flight_config: FlightConfig | FlightConfigDict = FlightConfig(),
        loglevel: (
            Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"] | None
        ) = "INFO",
    ):
        """
        Create a new Dremio instance.
        Its possible to set the authentification directly or later with the `auth` or `login` method.

        Parameters:
          hostname: hostname of the Dremio server (protocol and port optional).
          username: dremio username, e-mail.
          password: password or token from dremio ui.
          auth: authentification: _dremio<token>.
          token: token from POST /login on dremio api.
          modus: stand_alone or server, default: stand_alone
          port: port of the Dremio server, if None it will be auto set.
          protocol: https/http protocol of the Dremio server.
          loglevel: 'DEBUG'|'INFO'|'WARNING'|'ERROR'|'CRITICAL'|'NOTSET'|None to set the log level. Set None to prevent any setting.

        Returns:
          Dremio: Dremio instance.
        """
        v = self.v
        self.stand_alone = modus == "stand_alone"
        self.hostname: str = v.hostname(hostname, port, protocol)
        self.flight_config = (
            FlightConfig(**flight_config)
            if isinstance(flight_config, dict)
            else flight_config
        )
        self._headers: dict[str, str] = {"Content-Type": "application/json"}
        if loglevel:
            self.set_log_level(loglevel)
        if auth or token:
            if token:
                auth = f"_dremio{token}"
                self._token = token
            if not auth:
                raise AttributeError("")
            self._token = auth[7:]
            self._headers["Authorization"] = auth
            return
        if username and password:
            self.login(username, password)
            return

    def set_log_level(
        self,
        loglevel: (
            Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"] | None
        ) = None,
    ) -> int | None:
        """Set log level.

        Args:
            loglevel (Literal[&#39;DEBUG&#39;,&#39;INFO&#39;, &#39;WARNING&#39;, &#39;ERROR&#39;, &#39;CRITICAL&#39;, &#39;NOTSET&#39;] | None, optional): _description_. Defaults to None.

        Returns:
            int|None: logging.<LOGLEVEL> or None
        """
        if not loglevel:
            return None
        elif loglevel == "DEBUG":
            level = logging.DEBUG
        elif loglevel == "INFO":
            level = logging.INFO
        elif loglevel == "WARNING":
            level = logging.WARNING
        elif loglevel == "ERROR":
            level = logging.ERROR
        elif loglevel == "CRITICAL":
            level = logging.CRITICAL
        elif loglevel == "NOTSET":
            level = logging.NOTSET
        logging.basicConfig(level=level)
        return level

    # @staticmethod
    # def from_env(
    #     hostname: str = "DREMIO_HOSTNAME",
    #     username: str = "DREMIO_USERNAME",
    #     password: str = "DREMIO_PASSWORD",
    #     modus: str = "DREMIO_MODUS",
    #     port: str = "DREMIO_PORT",
    #     protocol: str = "DREMIO_PROTOCOL",
    #     *,
    #     flight_port: str = "DREMIO_FLIGHT_PORT",
    #     flight_tls: str = "DREMIO_FLIGHT_TLS",
    #     flight_path_to_certs: str = "DREMIO_FLIGHT_PATH_TO_CERTS",
    #     load_dotenv: bool = False,
    # ) -> "Dremio":
    #     """Create a new Dremio instance from environment variables. [learn more](https://github.com/continental/pydremio/blob/master/docs/DREMIO_LOGIN.md)
    #     The environment variables are:
    #       - DREMIO_HOSTNAME
    #       - DREMIO_USERNAME
    #       - DREMIO_PASSWORD
    #       - DREMIO_MODUS
    #       - DREMIO_PORT
    #       - DREMIO_PROTOCOL
    #       - DREMIO_FLIGHT_PORT
    #       - DREMIO_FLIGHT_TLS
    #       - DREMIO_FLIGHT_PATH_TO_CERTS


    #     Parameters:
    #       hostname: environment variable name for the hostname.
    #       username: environment variable name for the username.
    #       password: environment variable name for the password.
    #       modus: environment variable name for the modus.
    #       port: environment variable name for the port.
    #       protocol: environment variable name for the protocol.
    #       load_dotenv: load the dotenv file. Default: False.

    #     Returns:
    #       Dremio: Dremio instance.
    #     """
    #     if load_dotenv:
    #         from dotenv import load_dotenv as ldotenv

    #         ldotenv()
    #     import os

    #     hostname = os.getenv(hostname) or os.getenv("DREMIO_ENDPOINT")  # type: ignore
    #     username = os.getenv(username)  # type: ignore
    #     password = os.getenv(password)  # type: ignore
    #     modus = os.getenv(modus) or "stand_alone"
    #     port = os.getenv(port) or "443"
    #     protocol = os.getenv(protocol) or "https"
    #     if not hostname:
    #         raise ValueError("No hostname provided")
    #     if not username or not password:
    #         logging.warning("No username or password provided")
    #     if modus not in ["stand_alone", "server"]:
    #         raise ValueError("Modus must be either stand_alone or server")
    #     try:
    #         int(port)
    #     except:
    #         raise ValueError("Port must be a valid integer")
    #     if protocol not in ["https", "http"]:
    #         raise ValueError("Protocol must be either https or http")

    #     flight_conf: FlightConfigDict = {}
    #     if f_port := os.getenv(flight_port):
    #         flight_conf["port"] = int(f_port)
    #     if f_tls := os.getenv(flight_tls):
    #         flight_conf["tls"] = bool(f_tls)
    #     if f_path_to_certs := os.getenv(flight_path_to_certs):
    #         flight_conf["path_to_certs"] = str(f_path_to_certs)

    #     return Dremio(hostname, username=username, password=password, modus=modus, port=int(port), protocol=protocol, flight_config=flight_conf)  # type: ignore

    def auth(self, auth: Union[str, None] = None, token: Union[str, None] = None):
        """Set the authentification.
        Parameters:
          auth: authentification: _dremio<token>.
          token: token from POST /login on dremio api.
        Returns:
          Dremio: new logged in dremio instance.
        """
        modus: Literal["stand_alone", "server"] = (
            "stand_alone" if self.stand_alone else "server"
        )
        return Dremio(hostname=self.hostname, auth=auth, token=token, modus=modus)

    def login(self, username: str, password: str) -> CurrentUser:
        """
        Log in to Dremio. Its overwitring the current authentification.

        Parameters:
          username: dremio username, e-mail.
          password: password or token from dremio ui.

        Returns:
          CurrentUser: The current user.
        """
        v = self.v
        self.username = v(username=username)
        url = f"{self.hostname}/apiv2/login"
        payload = {"userName": username, "password": password}
        response = requests.post(url, json=payload)
        self._raise_error(response)
        user = CurrentUser(**response.json())
        self.username = user.userName
        self._token = user.token
        self._headers["Authorization"] = f"_dremio{user.token}"
        return user

    def _T(self, obj: dict, input: bool = False) -> CatalogObject:
        containerType = obj["entityType"]
        if input:
            T: Type = MAP_INPUT[containerType]
        else:
            # print(obj)
            T: Type = MAP_RESP[containerType]
        try:
            typed_object = cast(T, obj)
        except TypeError as e:
            raise TypeError(f"Error typing {obj} as {T}. {e}")
        return typed_object

    def _stand_alone_404(self, response: Response) -> bool:
        """This privat method is for stand alone use:
        If a resource is not found in stand alone mode, it should be returned None.
        On a server (not stand alone) 404 should be returned to the client.

        Args:
            response (Response): requests.Response

        Returns:
            bool: True, if in stand alone mode and response is 404
        """
        return self.stand_alone and response.status_code == 404

    def _raise_error(self, response: Response):
        status = response.status_code
        if status == 401:
            raise AuthError("Login incorrect or user not found")
        if status not in [200, 204]:
            try:
                raise DremioError(status_code=status, **response.json())
            except JSONDecodeError:
                # only if can not parse the Dremio Error
                raise DremioError(status_code=status, errorMessage="", moreInfo="")

    def _get_url_of_object(
        self,
        path: Union[str, list[str], None] = None,
        *,
        id: Union[UUID, str, None] = None,
    ) -> str:
        """Generate an url for any given Dremio Object.

        Args:
            path (Union[str,list[str],None], optional): path to Dremio Object
            id (Union[UUID,str,None], optional): id of Dremio Object

        Returns:
            str: url
        """
        obj = self._get_catalog_object(id, path)
        subpath = "/".join(path_to_list(obj["path"][:-1]))
        if obj["entityType"] == "folder":
            subpath += "/"
        if obj["entityType"] == "dataset":
            subpath += "."
        subpath += obj["path"][-1]
        return f"{self.hostname}/space/{subpath}"

    def _get_catalog_object(
        self,
        id: Union[UUID, str, None] = None,
        path: Union[str, list[str], None] = None,
    ) -> dict:  # CatalogObject|list[CatalogObject]:
        if not path and not id:
            raise ValueError("No path or id provided")
        url = f"{self.hostname}/api/v3/catalog/{str(id) or ''}"
        if path:
            path = path_to_list(path)
            if isinstance(path, list):
                path = "/".join(path)
            quoted_path = parse.quote(path).replace(" ", "%20").replace("%2F", "/")
            url = f"{self.hostname}/api/v3/catalog/by-path/{quoted_path}"
        response = requests.get(url, headers=self._headers)
        if response.status_code == 401:
            raise DremioError("Unauthorized", "request not allowed", status_code=401)
        self._raise_error(response)
        response.raise_for_status()
        result = response.json()
        if "data" in result:
            return result["data"]
        return result
