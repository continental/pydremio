__all__ = ["_MixinFlight"]  # this is like `export ...` in typescript
import copy
import logging
import pyarrow
from pyarrow.flight import (
    FlightClient,
    FlightStreamReader,
    FlightCallOptions,
    FlightDescriptor,
    FlightUnavailableError,
    FlightInfo,
)

from ..models import *

from . import BaseClass
from ._dataset import _MixinDataset


class _MixinFlight(_MixinDataset, BaseClass):

    @property
    def flight_url(self):
        return self.flight_config.uri(self.hostname)

    def _flight_client(
        self, flight_config: Optional[FlightConfig] = None
    ) -> FlightClient:
        """Get the pre-configured pyarrow.flight.FlightClient with optional configuration.

        Args:
            flight_config (Optional[FlightConfig], optional): Overwrite pre-config. Defaults to None.

        Returns:
            FlightClient: [pyarrow.flight.FlightClient](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html)
        """
        flight_config = flight_config or self.flight_config
        return FlightClient(
            location=(flight_config.uri(self.hostname)),
            disable_server_verification=flight_config.disable_certificate_verification,
            tls_root_certs=flight_config.tls_root_certs,  # type: ignore
        )

    def _flight_options(
        self, flight_config: Optional[FlightConfig] = None
    ) -> FlightCallOptions:
        flight_config = flight_config or self.flight_config
        return FlightCallOptions(
            headers=flight_config.get_headers(
                {"authorization": f"bearer {self._token}"},
                as_bytes=True,
            )  # type: ignore
        )

    def _flight_get_flight_info(
        self, descriptor: FlightDescriptor, options: FlightCallOptions | None = None
    ) -> FlightInfo:
        """Wrapper around pyarrow.flight.get_flight_info with pre-config.

        Args:
            descriptor (FlightDescriptor)
            options (FlightCallOptions | None, optional)

        Returns:
            FlightInfo
        """
        options = options or self._flight_options()
        return self._flight_client().get_flight_info(descriptor, options)

    def _flight_query_stream(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        flight_config: Optional[FlightConfig] = None,
        flight_options: Optional[FlightCallOptions] = None,
    ) -> FlightStreamReader:
        """Execute a SQL request and get the results as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          sql_request (str|SQLRequest): SQL request as SQLRequest object.
          flight_config (FlightConfig | None): Optional: Config for flight. Leave empty to use `Dremio.flight_config`.

        Returns:
          pyarrow.flight.FlightStreamReade: The job results as [pyarrow.flight.FlightStreamReade](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamReader.htm).
        """
        if isinstance(sql_request, str):
            sql_request = SQLRequest(sql_request)
        flight_config = flight_config or self.flight_config

        client = self._flight_client(flight_config)
        options = flight_options or self._flight_options(flight_config)

        try:
            flight_info = client.get_flight_info(
                FlightDescriptor.for_command(sql_request.sql), options
            )
        except FlightUnavailableError as e:
            if not flight_config.allow_autoconfig:
                raise e
            if "SETTINGS" in str(e) and flight_config.tls == False:
                logging.warning(
                    "Got FlightUnavailableError. Retrying with `flight_config.tls = True`..."
                )
                flight_config_copy = copy.copy(flight_config)
                flight_config_copy.tls = True
                results = self._flight_query_stream(
                    sql_request=sql_request, flight_config=flight_config_copy
                )
                logging.warning(
                    "Retry with `flight_config.tls = True` was successful. Set `Dremio.flight_config.tls = True` for future queries."
                )
                return results
            if (
                "empty address list" in str(e)
                and self.flight_config.disable_certificate_verification == False
            ):
                logging.warning(
                    "Got FlightUnavailableError. Retry with `flight_config.disable_certificate_verification = True`..."
                )
                flight_config_copy = copy.copy(flight_config)
                flight_config_copy.disable_certificate_verification = True
                results = self._flight_query_stream(
                    sql_request=sql_request, flight_config=flight_config_copy
                )
                logging.warning(
                    "Retry with `flight_config.disable_certificate_verification = True` was successful. Set `Dremio.flight_config.disable_certificate_verification = True` for future queries."
                )
                return results
            raise e
        return client.do_get(flight_info.endpoints[0].ticket, options)

    def _flight_query(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        flight_config: Optional[FlightConfig] = None,
        flight_options: Optional[FlightCallOptions] = None,
    ) -> pyarrow.Table:
        """Execute a SQL request and get the results as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          sql_request (str|SQLRequest): SQL request as SQLRequest object.
          flight_config (FlightConfig | None): Optional: Config for flight. Leave empty to use `Dremio.flight_config`.

        Returns:
          pyarrow.Table: The job results as pyarrow.Table.
        """
        return self._flight_query_stream(
            sql_request=sql_request,
            flight_config=flight_config,
            flight_options=flight_options,
        ).read_all()

    def _flight_query_dataset(
        self,
        dataset: Dataset | list[str] | str | None = None,
        *,
        id: Union[UUID, str, None] = None,
        flight_config: Optional[FlightConfig] = None,
        flight_options: Optional[FlightCallOptions] = None,
    ) -> pyarrow.Table:
        """Get the dataset as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          dataset: Dataset | path: Instance of a Dataset or "A.B.C" or ["A","B","C"].
          id: dataset uuid.

        Returns:
          pyarrow.Table: The job results as pyarrow.Table.
        """
        if not dataset:
            if not id:
                raise TypeError("A query on a dataset needs path or id")
            dataset = self.get_dataset(id=id)
        if isinstance(dataset, list) or isinstance(dataset, str):
            dataset = self.get_dataset(path=dataset)
        if not isinstance(dataset, Dataset):
            raise TypeError("Dataset is needed to run a dataset")

        if not dataset.sql:
            raise DremioConnectorError(
                "No SQL statement in dataset",
                f"Please check the sql statement in dataset {dataset.id}",
            )
        return self._flight_query(
            dataset.sql,
            flight_config=flight_config,
            flight_options=flight_options,
        )

    @property
    def flight(self):
        """Namespace: Collection of methods related to arrow flight.

        Returns:
            Flight: Namespace of arrow flight related methods
        """
        return Flight(self)


class Flight:
    """Namespace: Collection of methods related to arrow flight."""

    def __init__(self, mixin: _MixinFlight):
        self.query = mixin._flight_query
        self.query_stream = mixin._flight_query_stream
        self.query_dataset = mixin._flight_query_dataset
        self.config = mixin.flight_config
        self.url = mixin.flight_url
        self.get_client = mixin._flight_client
        self.get_options = mixin._flight_options
        self.get_flight_info = mixin._flight_get_flight_info

    @property
    def client(self) -> FlightClient:
        """Shortcut for get_client() with default config

        Returns:
            FlightClient: [pyarrow.flight.FlightClient](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html)
        """
        return self.get_client()

    @property
    def options(self) -> FlightCallOptions:
        """Shortcut for get_options() with default config."""
        return self.get_options()
