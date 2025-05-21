from uuid import UUID
from pyarrow import Table
import pytest
from dremio.models.jobs import Job, JobResult
from tests.testutils.helper import random_name
from src.dremio import Dremio, JobResult, Dataset
from pyarrow.flight import FlightStreamReader


def test_query(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    result_arrow = dremio.query(sql, "arrow")
    assert isinstance(result_arrow, JobResult)

    result_http = dremio.query(sql, "http")
    assert isinstance(result_http, JobResult)

    assert result_arrow.rowCount == result_http.rowCount


def test__flight_query_stream(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    stream = dremio._flight_query_stream(sql)
    assert isinstance(stream, FlightStreamReader)


def test_flight_query_stream(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    stream = dremio.flight.query_stream(sql)
    assert isinstance(stream, FlightStreamReader)


def test__flight_query(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    table = dremio._flight_query(sql)
    assert isinstance(table, Table)


def test_flight_query(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    table = dremio.flight.query(sql)
    assert isinstance(table, Table)


def test_flight_query_dataset(dremio: Dremio, dataset: Dataset):
    table = dremio._flight_query_dataset(dataset)
    assert isinstance(table, Table)


def test__flight_query_dataset(dremio: Dremio, dataset: Dataset):
    table = dremio.flight.query_dataset(dataset)
    assert isinstance(table, Table)


def test_flight_query_equal(dremio: Dremio, dataset: Dataset):
    table_sql = dremio.flight.query(dataset.sql or "")
    table_ds = dremio.flight.query_dataset(dataset)

    assert table_ds.schema == table_sql.schema
    assert table_ds.num_rows == table_ds.num_rows


def test_http_query(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    result, job = dremio.http.query(sql)
    assert job.jobState == "COMPLETED"
    assert isinstance(result, JobResult)


def test_http_query_dataset(dremio: Dremio, dataset: Dataset):
    result, job = dremio.http.query_dataset(dataset)
    assert job.jobState == "COMPLETED"
    assert isinstance(result, JobResult)
