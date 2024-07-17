from fractal_server.string_tools import __SPECIAL_CHARACTERS__
from fractal_server.string_tools import sanitize_string


def test_unit_sanitize_string():
    for value in __SPECIAL_CHARACTERS__:
        sanitized_value = sanitize_string(value)
        assert sanitized_value == "_"

    value = "/some (rm) \t path *!"
    expected_value = "_some__rm____path___"
    assert sanitize_string(value) == expected_value
