__all__ = ["_MixinSQL"]  # this is like `export ...` in typescript
import logging
from typing import overload
import requests
from time import sleep

from ..utils.multithreading import ThreadWithReturnValue as Thread

from ..utils.converter import to_dict, path_to_dotted
from ..models import *
from ..models.jobs import SQLType

from . import BaseClass
from ._catalog import _MixinCatalog


class _MixinSQL(_MixinCatalog, BaseClass):

    def _http_query(self, sql_request: Union[str, SQLRequest]) -> UUID:
        """Execute SQL query on Dremio.
        Please use `Dremio.start_job(...)` if you simply run a sql query.
        This Method is for low level implementation.

        Args:
            sql_request (Union[str, SQLRequest]): SQL statement as string or SQLRequest object

        Returns:
            UUID: job id
        """
        url = f"{self.hostname}/api/v3/sql"
        if isinstance(sql_request, str):
            sql_request = SQLRequest(sql_request)
        payload = to_dict(sql_request)
        response = requests.post(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return JobId(**response.json()).id

    @overload
    def _http_query_result(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
        return_exception: Literal[False] = False,
    ) -> tuple[JobResult, Job]:
        """Execute a SQL request and get the results.
        If you need the Exception as return value, set `return_exception=True`.

        Parameters:
          sql_request: SQL request as SQLRequest object.
          limit: The limit of the results rows for pagination.
          offset: The offset of the results rows pagination.
          timeout_in_sec: seconds until TimeoutError raising
          return_exception: overload this method to return the exception instead of raising it. Default: False

        Raises:
          DremioError

        Returns:
          tuple[JobResult|None, Job]: The job results and the job.
        """
        ...

    @overload
    def _http_query_result(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
        return_exception: Literal[True] = True,
    ) -> tuple[Union[JobResult, None], Union[Job, None], Union[Exception, None]]:
        """Execute a SQL request and get the results. This method don't raise errors.
        Overloaded!!!

        Parameters:
          sql_request: SQL request as SQLRequest object.
          limit: The limit of the results rows for pagination.
          offset: The offset of the results rows pagination.
          timeout_in_sec: seconds until TimeoutError raising
          return_exception: overload this method to return the exception instead of raising it.

        Returns:
          tuple[JobResult|None, Job]: The job results and the job.
        """
        ...

    def _http_query_result(
        self,
        sql_request: Union[str, SQLRequest],
        *,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
        return_exception: Literal[True, False] = False,
    ):
        if return_exception:
            # only for special cases, when you need only the job and the error, but not the result
            # please use sql_result for normal cases
            result: Union[JobResult, None] = None
            error: Union[Exception, None] = None
            job: Union[Job, None] = None
            try:
                job_id = self._http_query(sql_request)
                result = self._http_wait_for_job_result(
                    job_id, limit, offset, timeout_in_sec
                )
            except Exception as e:
                error = e
            try:
                job = self.get_job_info(job_id)
            except Exception as e:
                error = e
                job = None
            return result, job, error
        else:
            job_id = self._http_query(sql_request)
            result = self._http_wait_for_job_result(
                job_id, limit, offset, timeout_in_sec
            )
            job = self.get_job_info(job_id)
            return result, job

    def _http_start_job(self, sql_request: Union[str, SQLRequest]) -> Job:
        """Start a sql job over http.
        This method is for low level implementations.

        Args:
            sql_request (Union[str, SQLRequest]): sql query as string or SQLRequest object.

        Returns:
            Job: job object of the started job
        """
        job_id = self._http_query(sql_request=sql_request)
        return self.get_job_info(job_id)

    def _http_start_job_on_dataset(
        self, id: Union[UUID, str], limit: Union[int, None] = None
    ) -> Job:
        """Start a job on a dataset. Is necessary for Dremio.Dataset.run()

        Args:
            id (Union[UUID, str]): dataset id
            limit (Union[int, None], optional): row limit for request. Defaults to None.

        Raises:
            ValueError: DremioError

        Returns:
            UUID: job id
        """
        item = self.get_catalog_by_id(id)
        if type(item) != Dataset:
            raise ValueError("requested resource is not dataset")
        q = f"SELECT * FROM {path_to_dotted(item.path)}"
        if limit:
            q += " LIMIT {limit}"
        sql_req = SQLRequest(q)
        job_id = self._http_query(sql_req)
        return self.get_job_info(job_id)

    def _http_dataset_result(
        self,
        dataset: Dataset | list[str] | str,
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 1000,
    ) -> tuple[JobResult, Job]:
        """Get the result of a dataset."""
        if isinstance(dataset, list) or isinstance(dataset, str):
            dataset = cast(Dataset, self._get_catalog_object(path=dataset))
        if not isinstance(dataset, Dataset):
            raise TypeError("Dataset is needed to run a dataset")
        if not dataset.sql:
            raise DremioConnectorError(
                "No SQL statement in dataset",
                f"please check the sql statement in dataset {dataset.id}",
            )
        return self._http_query_result(
            SQLRequest(dataset.sql, dataset.sqlContext),
            limit=limit,
            offset=offset,
            timeout_in_sec=timeout_in_sec,
        )

    def get_job_info(self, id: Union[UUID, str]) -> Job:
        """Get the job info.

        Parameters:
          id: job uuid.

        Returns:
          Job: job object.
        """
        url = f"{self.hostname}/api/v3/job/{str(id)}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        job = cast(Job, response.json())
        job.id = id  # type: ignore
        return job

    def cancel_job(self, id: Union[UUID, str]) -> Job:
        """Cancel the job.

        Parameters:
          id: job uuid.

        Returns:
          Job: job object.
        """
        url = f"{self.hostname}/api/v3/job/{str(id)}/cancel"
        response = requests.post(url, headers=self._headers)
        self._raise_error(response)
        # check job status
        job: Job = self.get_job_info(id)
        if job.jobState != "CANCELED":
            raise DremioError(
                "Job not canceled", f"Job {id} still in state {job.jobState}"
            )
        return job

    def _http_get_job_result(
        self, id: Union[UUID, str], limit: Union[int, None] = None, offset: int = 0
    ) -> JobResult:
        """ONLY FOR LOW LEVEL IMPLEMENTATIONS! Use Dremio.query() otherwise!
        Get the job results. DID NOT WAIT FOR THE RESULTS! Use `wait_for_job_result` instead.

        Parameters:
          id: The job id.
          limit: The limit of the results rows for pagination.
          offset: The offset of the results rows pagination.

        Returns:
          JobResult: The job results.
        """
        url = f"{self.hostname}/api/v3/job/{str(id)}/results?offset={offset}"
        if limit:
            url += f"&limit={limit}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        result = JobResult.from_dict(response.json())
        if offset != 0 or limit:
            return result

        # paginate through all result rows
        threads: list[Thread] = []
        for i in range(len(result.rows), result.rowCount, 500):
            x = Thread(target=self._http_get_job_result, args=(id, 500, i))
            threads.append(x)
            x.start()
        for thread in threads:
            r: Union[JobResult, None] = thread.join()
            if not r:
                raise Exception(
                    f"Thread failed: {id}[{i}:{min(i+500,result.rowCount)}]"
                )
            result.rows += Schema[SQLType](r.rows)
        return result

    def _http_wait_for_job_result(
        self,
        id: Union[UUID, str],
        limit: Union[int, None] = None,
        offset: int = 0,
        timeout_in_sec: int = 500,
    ) -> JobResult:
        """Waits for the job results and give them back.
        **Attention**: this method can raise errors.

        Parameters:
          id: The job id.
          limit: The limit of the results rows for pagination.
          offset: The offset of the results rows pagination.
          timeout_in_sec: seconds until TimeoutError raising

        Returns:
          JobResult: The job results.
        """
        for ms in [50, 100, 100, 250, 250, 500, 750] + timeout_in_sec * [1000]:
            job: Job = self.get_job_info(id)
            if job.jobState in ["COMPLETED", "CANCELED", "FAILED"]:
                break
            sleep(ms / 1000)
        else:
            raise TimeoutError(f"Job {job.id} still {job.jobState}")
        if job.jobState == "COMPLETED":
            return self._http_get_job_result(id, limit, offset)
        else:
            raise DremioError(
                f"Job {job.id} {job.jobState} {job.errorMessage}",
                f"{job.cancellationReason}",
            )

    @property
    def http(self) -> "HTTP":
        """Namespace: Collection of methods related to data fetching via http.
        It's highly recommended to use Dremio.flight instead!!!

        Returns:
            HTTP: Namespace of http data fetching methods
        """
        return HTTP(self)


class HTTP:
    """Namespace: Collection of methods related to data fetching via http."""

    def __init__(self, mixin: _MixinSQL):
        self.query = mixin._http_query_result
        self.start_job = mixin._http_start_job
        self.query_dataset = mixin._http_dataset_result
