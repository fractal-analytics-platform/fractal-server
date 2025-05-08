from functools import cmp_to_key

import pytest

from fractal_server.string_tools import __NOT_ALLOWED_FOR_COMMANDS__
from fractal_server.string_tools import __SPECIAL_CHARACTERS__
from fractal_server.string_tools import is_version_greater_than
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


def test_is_version_greater_than_than():

    versions = [
        "2",
        "0.10.0c0",
        "0.10.0b4",
        "0.10.0",
        "0.10.0alpha3",
        "0.10.0a2",
        "1.0.0",
        "0.10.0a0",
        "1.0.0rc4.dev7",
        "0.10.0beta5",
        "0.10.0alpha0",
        "0.1.2",
        "0.1.dev27+g1458b59",
        "0.2.0a0",
    ]

    expected_sorted_versions = [
        "0.1.dev27+g1458b59",
        "0.1.2",
        "0.2.0a0",
        "0.10.0a0",
        "0.10.0alpha0",
        "0.10.0a2",
        "0.10.0alpha3",
        "0.10.0b4",
        "0.10.0beta5",
        "0.10.0c0",
        "0.10.0",
        "1.0.0rc4.dev7",
        "1.0.0",
        "2",
    ]

    def compare_versions(a, b):
        if is_version_greater_than(a, b):
            return 1
        elif is_version_greater_than(b, a):
            return -1
        else:
            return 0

    sorted_versions = sorted(versions, key=cmp_to_key(compare_versions))
    assert sorted_versions == expected_sorted_versions
