import pytest

from src.dremio.utils.validation import Validation


def test_hostname_https():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, None, "https")
    assert res == "https://dremio.com"


def test_hostname_http():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, None, "http")
    assert res == "http://dremio.com"


def test_hostname_http_port():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, 80, "http")
    assert res == "http://dremio.com"


def test_hostname_https_port():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, 443, "https")
    assert res == "https://dremio.com"


def test_hostname_https_port80():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, 80, "https")
    assert res == "https://dremio.com:80"


def test_hostname_http_port443_0():
    hostname = "dremio.com"
    res = Validation().hostname(hostname, 443, "http")
    assert res == "http://dremio.com:443"


def test_hostname_http_port443_1():
    hostname = "https://dremio.com"
    res = Validation().hostname(hostname, 443, "http")
    assert res == "http://dremio.com:443"


def test_hostname_none_none():
    hostname = "http://dremio.com:443"
    res = Validation().hostname(hostname)
    assert res == "http://dremio.com:443"
