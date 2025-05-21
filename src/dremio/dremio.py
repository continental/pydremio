#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from dremio.models.flight_config import FlightConfigDict
from .version import *  # for __version__, __author__

__all__ = ["Dremio"]

from ._mixins import (
    BaseClass,
    _MixinCatalog,
    _MixinSQL,
    _MixinQuery,
    _MixinDataset,
    _MixinFolder,
    _MixinFlight,
    _MixinReflection,
    _MixinUser,
    _MixinRole,
)


# TODO: write a few lines about mixins
class Dremio(
    _MixinRole,
    _MixinUser,
    _MixinQuery,
    _MixinFlight,
    _MixinFolder,
    _MixinDataset,
    _MixinReflection,
    _MixinSQL,
    _MixinCatalog,
    BaseClass,
):
    """This class is the main class of the Dremio connector. [learn more](https://github.com/continental/pydremio/blob/master/docs/DREMIO_METHODS.md)"""

    @staticmethod
    def from_env(
        hostname: str = "DREMIO_HOSTNAME",
        username: str = "DREMIO_USERNAME",
        password: str = "DREMIO_PASSWORD",
        modus: str = "DREMIO_MODUS",
        port: str = "DREMIO_PORT",
        protocol: str = "DREMIO_PROTOCOL",
        *,
        flight_port: str = "DREMIO_FLIGHT_PORT",
        flight_tls: str = "DREMIO_FLIGHT_TLS",
        flight_path_to_certs: str = "DREMIO_FLIGHT_PATH_TO_CERTS",
        load_dotenv: bool = False,
    ) -> "Dremio":
        """Create a new Dremio instance from environment variables. [learn more](https://github.com/continental/pydremio/blob/master/docs/DREMIO_LOGIN.md)
        The environment variables are:
          - DREMIO_HOSTNAME
          - DREMIO_USERNAME
          - DREMIO_PASSWORD
          - DREMIO_MODUS
          - DREMIO_PORT
          - DREMIO_PROTOCOL
          - DREMIO_FLIGHT_PORT
          - DREMIO_FLIGHT_TLS
          - DREMIO_FLIGHT_PATH_TO_CERTS


        Parameters:
          hostname: environment variable name for the hostname.
          username: environment variable name for the username.
          password: environment variable name for the password.
          modus: environment variable name for the modus.
          port: environment variable name for the port.
          protocol: environment variable name for the protocol.
          load_dotenv: load the dotenv file. Default: False.

        Returns:
          Dremio: Dremio instance.
        """
        if load_dotenv:
            from dotenv import load_dotenv as ldotenv
            ldotenv()
        import os

        hostname = os.getenv(hostname) or os.getenv("DREMIO_ENDPOINT")  # type: ignore
        username = os.getenv(username)  # type: ignore
        password = os.getenv(password)  # type: ignore
        modus = os.getenv(modus) or "stand_alone"
        port = os.getenv(port) or "443"
        protocol = os.getenv(protocol) or "https"
        if not hostname:
            raise ValueError("No hostname provided")
        if not username or not password:
            logging.warning("No username or password provided")
        if modus not in ["stand_alone", "server"]:
            raise ValueError("Modus must be either stand_alone or server")
        try:
            int(port)
        except:
            raise ValueError("Port must be a valid integer")
        if protocol not in ["https", "http"]:
            raise ValueError("Protocol must be either https or http")

        flight_conf: FlightConfigDict = {}
        if f_port := os.getenv(flight_port):
            flight_conf["port"] = int(f_port)
        if f_tls := os.getenv(flight_tls):
            flight_conf["tls"] = bool(f_tls)
        if f_path_to_certs := os.getenv(flight_path_to_certs):
            flight_conf["path_to_certs"] = str(f_path_to_certs)

        return Dremio(hostname, username=username, password=password, modus=modus, port=int(port), protocol=protocol, flight_config=flight_conf)  # type: ignore