import pytest

from fractal_server.string_tools import __NOT_ALLOWED_FOR_COMMANDS__
from fractal_server.string_tools import __SPECIAL_CHARACTERS__
from fractal_server.string_tools import sanitize_string
from fractal_server.string_tools import validate_cmd


def test_unit_sanitize_string():
    for value in __SPECIAL_CHARACTERS__:
        sanitized_value = sanitize_string(value)
        assert sanitized_value == "_"

    value = "/some (rm) \t path *!"
    expected_value = "_some__rm____path___"
    assert sanitize_string(value) == expected_value


def test_unit_validate_cmd():
    for char in __NOT_ALLOWED_FOR_COMMANDS__:
        cmd = f"abc{char}def"
        with pytest.raises(ValueError):
            validate_cmd(cmd)
        validate_cmd(cmd, allow_char=f"xy{char}z")

    with pytest.raises(
        ValueError,
        match="MyAttribute must not contain",
    ):
        validate_cmd("; rm", attribute_name="MyAttribute")
