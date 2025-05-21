__all__ = ["FlightConfig", "FlightConfigDict"]


from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional, TypedDict, overload

import certifi

from ..utils.parser import cut_scheme


@dataclass
class FlightConfig:
    """This config is to configure arrow flight queries to Dremio.

    Properties:
      port (int): Port of the grpc endpoint of Dremio. Default: 32010.
      tls (bool): Enable to use TLS. If `https` set as protocol for dremio, it is it is likely enabled.
      disable_certificate_verification: Disable to use certs from `path_to_certs`.
      allow_autoconfig (bool): Disable to prevent automatic tls setting.
      path_to_certs: Path to certs like cacert.pem. Default: certifi.where().
      engine: Routing engine for Dremio Cloud. Default: None.
    """

    port: int = 32010
    tls: bool = False
    disable_certificate_verification: bool = False
    allow_autoconfig: bool = True
    path_to_certs: str = certifi.where()
    headers: dict[str, Any] = field(default_factory=lambda: {})
    session_properties: dict[str, Any] = field(default_factory=lambda: {})
    engine: Optional[str] = None
    _scheme: Optional[str] = None

    @property
    def tls_root_certs(self) -> bytes | None:
        if not self.path_to_certs:
            return None
        with open(self.path_to_certs, "rb") as f:
            return f.read()

    @property
    def scheme(self) -> str:
        if self._scheme:
            return self._scheme
        p = "grpc"
        # if not self.disable_certificate_verification:
        #     p += "+ssl"
        if self.tls:
            p += "+tls"
        return p

    @scheme.setter
    def scheme(self, v: str | None):
        self._scheme = v

    def uri(self, hostname: str) -> str:
        return f"{self.scheme}://{cut_scheme(hostname)}:{self.port}"

    @overload
    def get_headers(
        self, headers: dict[str, Any] = {}, as_bytes: Literal[False] = False
    ) -> list[tuple[str, str]]: ...

    @overload
    def get_headers(
        self, headers: dict[str, Any] = {}, as_bytes: Literal[True] = True
    ) -> list[tuple[bytes, bytes]]: ...

    def get_headers(
        self, headers: dict[str, Any] = {}, as_bytes: bool = False
    ) -> list[tuple[bytes, bytes]] | list[tuple[str, str]]:
        if as_bytes:
            return [
                (k.encode("utf-8"), f"{v}".encode("utf-8"))
                for k, v in (self.session_properties | self.headers | headers).items()
            ]
        return [
            (k, f"{v}")
            for k, v in (self.session_properties | self.headers | headers).items()
        ]


class FlightConfigDict(TypedDict, total=False):
    port: int
    tls: bool
    disable_certificate_verification: bool
    allow_autoconfig: bool
    path_to_certs: str
    engine: str
    headers: dict[str, Any]
