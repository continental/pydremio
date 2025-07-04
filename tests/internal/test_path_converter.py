import pytest
from src.dremio.utils.converter import path_to_list, path_to_dotted

def test_path_to_list():
    assert path_to_list("a.b.c") == ["a", "b", "c"]

def test_path_to_dotted():
    assert path_to_dotted("a.b.c") == '"a"."b"."c"'
