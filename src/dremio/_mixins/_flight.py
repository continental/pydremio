__all__ = ["_MixinFlight"]  # this is like `export ...` in typescript
import copy
import logging
import pyarrow as pa
from pyarrow import flight
from pyarrow.flight import FlightClient, FlightUnavailableError

from ..models import *

from . import BaseClass
from ._dataset import _MixinDataset


class _MixinFlight(_MixinDataset, BaseClass):

    @property
    def flight_url(self):
        return self.flight_config.uri(self.hostname)

    def _flight_query_stream(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        flight_config: Optional[FlightConfig] = None,
    ) -> flight.FlightStreamReader:
        """Execute a SQL request and get the results as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          sql_request (str|SQLRequest): SQL request as SQLRequest object.
          flight_config (FlightConfig | None): Optional: Config for flight. Leave empty to use `Dremio.flight_config`.

        Returns:
          fliegt.FlightStreamReader: The job results as flight data.
        """
        if isinstance(sql_request, str):
            sql_request = SQLRequest(sql_request)
        flight_config = flight_config or self.flight_config

        client: FlightClient = FlightClient(
            location=(flight_config.uri(self.hostname)),
            disable_server_verification=flight_config.disable_certificate_verification,
            tls_root_certs=flight_config.tls_root_certs,
        )
        options = flight.FlightCallOptions(
            headers=flight_config.get_headers(
                {"authorization": f"bearer {self._token}"}
            )
        )
        try:
            flight_info = client.get_flight_info(
                flight.FlightDescriptor.for_command(sql_request.sql), options
            )
        except flight.FlightUnavailableError as e:
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
    ) -> pa.Table:
        """Execute a SQL request and get the results as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          sql_request (str|SQLRequest): SQL request as SQLRequest object.
          flight_config (FlightConfig | None): Optional: Config for flight. Leave empty to use `Dremio.flight_config`.

        Returns:
          pa.Table: The job results as pyarrow table.
        """
        return self._flight_query_stream(
            sql_request=sql_request, flight_config=flight_config
        ).read_all()

    def _flight_query_dataset(
        self,
        path: Union[list[str], str, None] = None,
        *,
        id: Union[UUID, str, None] = None,
    ) -> pa.Table:
        """Get the dataset as flight data. [learn more](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)
        You can use the `.to_pandas()` method on the result to convert it to a pandas DataFrame.
        Use Dataset.run() for easier handling.

        Parameters:
          path: dataset path: "A.B.C" or ["A","B","C"].
          id: dataset uuid.

        Returns:
          pa.Table: The job results as pyarrow table.
        """
        dataset = self.get_dataset(path=path, id=id)
        if not dataset.sql:
            raise DremioConnectorError(
                "No SQL statement in dataset",
                f"Please check the sql statement in dataset {dataset.id}",
            )
        return self._flight_query_stream(dataset.sql)
