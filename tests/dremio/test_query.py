import pytest
from dremio.models.jobs import JobResult
from tests.testutils.helper import random_name
from src.dremio import Dremio, Folder, JobResult


def test_query(dremio: Dremio, sample_dataset_paths: list[str]):
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    result_arrow = dremio.query(sql, "arrow")
    assert isinstance(result_arrow, JobResult)

    result_http, job = dremio.query(sql, "http")
    assert job.jobState == "COMPLETED"
    assert isinstance(result_http, JobResult)

    assert result_arrow.rowCount == result_http.rowCount
