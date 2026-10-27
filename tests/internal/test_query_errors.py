import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
from pyarrow.flight import FlightServerError, FlightUnavailableError
import pytest

from dremio import Dremio
from dremio.models import JobResult


@pytest.fixture
def client(monkeypatch):
    client = Dremio.__new__(Dremio)
    monkeypatch.setattr(client, "_flight_query", Mock())
    monkeypatch.setattr(client, "_http_start_job", Mock())
    return client


@pytest.mark.parametrize(
    "error",
    [
        FlightServerError("Dataset already exists"),
        FlightUnavailableError("Server unavailable"),
        RuntimeError("Query rejected"),
    ],
)
def test_arrow_failure_reports_original_cause(client, caplog, error):
    client._flight_query.side_effect = error

    with caplog.at_level(logging.WARNING), pytest.raises(type(error)) as caught:
        client.query("CREATE TABLE example AS SELECT 1")

    assert caught.value is error
    assert str(error) in caplog.text
    assert "Check your config" not in caplog.text
    assert "try again over http" not in caplog.text
    client._http_start_job.assert_not_called()
    # Re-raising should not add a second query frame to the traceback.
    frames = []
    traceback = caught.value.__traceback__
    while traceback is not None:
        frames.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    assert frames.count("query") == 1


def test_arrow_result_conversion_error_is_not_a_transport_failure(
    client, monkeypatch, caplog
):
    error = ValueError("Unsupported result field")
    client._flight_query.return_value = pa.table({"value": [1]})
    monkeypatch.setattr(JobResult, "from_arrow_table", Mock(side_effect=error))

    with caplog.at_level(logging.WARNING), pytest.raises(ValueError) as caught:
        client.query("SELECT 1")

    assert caught.value is error
    assert not caplog.records
    client._http_start_job.assert_not_called()


def test_arrow_success_still_converts_the_table(client, caplog):
    client._flight_query.return_value = pa.table({"name": ["München"], "value": [2]})

    result = client.query("SELECT name, value FROM example")

    assert result.rowCount == 1
    assert result.rows == [{"name": "München", "value": 2}]
    assert not caplog.records
    client._http_start_job.assert_not_called()


def test_http_query_still_forwards_pagination(client, monkeypatch):
    client._http_start_job.return_value = SimpleNamespace(id="test-job")
    wait = Mock(return_value=object())
    monkeypatch.setattr(client, "_http_wait_for_job_result", wait)

    result = client.query("SELECT 1", "http", limit=5, offset=2, timeout_in_sec=10)

    assert result is wait.return_value
    wait.assert_called_once_with(id="test-job", limit=5, offset=2, timeout_in_sec=10)
    client._flight_query.assert_not_called()


def test_invalid_method_does_not_execute_a_query(client):
    with pytest.raises(ValueError, match="method must be"):
        client.query("SELECT 1", "invalid")

    client._flight_query.assert_not_called()
    client._http_start_job.assert_not_called()
