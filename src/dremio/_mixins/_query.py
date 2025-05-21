__all__ = ["_MixinQuery"]  # this is like `export ...` in typescript
import logging
from typing import overload
from polars import sql
import pyarrow as pa
from pyarrow import flight
from pyarrow.flight import FlightClient

from ..models import *

from . import BaseClass
from ._dataset import _MixinDataset
from ._sql import _MixinSQL
from ._flight import _MixinFlight


class _MixinQuery(_MixinFlight, _MixinDataset, _MixinSQL, BaseClass):
    "This is a abstraction of arrow flight and sql for easer usability."

    @overload
    def query(
        self,
        sql_request: Union[str, SQLRequest],
        method: Literal["arrow"] = "arrow",
    ) -> JobResult:
        """Query Dremio data with SQL.

        Args:
            sql_request (Union[str, SQLRequest]): SQL statement as string or Dremio SQL-object.
            method (Literal[&quot;arrow&quot;, &quot;http&quot;], optional): Query method. Defaults to "arrow".

        Raises:
            DremioConnectorError: Something went wrong while job execution.
            Exception: Any error from arrow flight.

        Returns:
            JobResult
        """

    @overload
    def query(
        self,
        sql_request: Union[str, SQLRequest],
        method: Literal["http"] = "http",
        *,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
    ) -> JobResult:
        """Execute a SQL request and get the results. This method don't raise errors.
        Overloaded!!!

        Args:
            sql_request: SQL request as SQLRequest object.
            limit: The limit of the results rows for pagination.
            offset: The offset of the results rows pagination.
            timeout_in_sec: seconds until TimeoutError raising
            timeout_in_sec (int, optional): _description_. Defaults to 1000.

        Returns:
          tuple[JobResult|None, Job]: The job results and the job.
        """

    def query(
        self,
        sql_request: Union[str, SQLRequest],
        method: Literal["arrow", "http"] = "arrow",
        *,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
    ) -> JobResult:
        if method == "http":
            job = self._http_start_job(
                sql_request=sql_request,
            )
            if not job.id:
                raise DremioConnectorError(
                    "Error while starting job on sql query",
                    f'Failed to start job on "{sql_request}"',
                )
            return self._http_wait_for_job_result(
                id=job.id,
                limit=limit,
                offset=offset,
                timeout_in_sec=timeout_in_sec,
            )
        elif method == "arrow":
            try:
                table: pa.Table = self._flight_query(sql_request=sql_request)
                return JobResult.from_arrow_table(table)
            except Exception as e:
                logging.warning(
                    "Query over arrow flight failed. Check your config or try again over http."
                )
                raise e
        else:
            raise ValueError('method must be "arrow" or "http"')
