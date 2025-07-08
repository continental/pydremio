import pytest
from src.dremio.utils.converter import path_to_list, path_to_dotted

@pytest.mark.parametrize(
        "input_path,expected",
        [
            pytest.param(["a", "b", "c "], ["a", "b", "c "], id="list_input"),
            pytest.param("[a, b, c ]", ["a", "b", "c "], id="string_input"),
            pytest.param("[a, b, c and d ]", ["a", "b", "c and d "], id="string_with_spaces_input"),
            pytest.param("[a, b, c and' d ]", ["a", "b", "c and' d "], id="string_with_quotes_input"),
            pytest.param("[a, b, c.d ]", ["a", "b", "c.d "], id="string_with_dots_input"),
            pytest.param("[a, b#, c /and' @ d ]", ["a", "b#", "c /and' @ d "], id="string_with_special_chars_input"),
            pytest.param("a.b.c", ["a", "b", "c"], id="dotted_input"),
        ])
def test_path_to_list(input_path, expected):
    assert path_to_list(input_path) == expected

@pytest.mark.parametrize(
        "input_path,expected",
            [
                pytest.param("a.b.c", '"a"."b"."c"', id="three_segments"),
                pytest.param('a.b."c.d"', '"a"."b"."c.d"', id="with_dot_in_name"),
                pytest.param("x.y", '"x"."y"', id="two_segments"),
                pytest.param("", '""', id="empty_string"),
                pytest.param("one", '"one"', id="single_segment"),
            ])
def test_path_to_dotted(input_path, expected):
    assert path_to_dotted(input_path) == expected