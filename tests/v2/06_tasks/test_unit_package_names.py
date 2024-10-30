import pytest

from fractal_server.tasks.utils import normalize_package_name
from fractal_server.tasks.v2.utils_package_names import _parse_wheel_filename


def test_parse_wheel_filename():
    with pytest.raises(
        ValueError,
        match="Input must be a filename, not a full path",
    ):
        _parse_wheel_filename(wheel_filename="/tmp/something.whl")


def test_compare_package_names():
    # TODO
    raise NotImplementedError()


def test_normalize_package_name():
    """
    Test based on the example in
    https://packaging.python.org/en/latest/specifications/name-normalization.
    """
    inputs = (
        "friendly-bard",
        "Friendly-Bard",
        "FRIENDLY-BARD",
        "friendly.bard",
        "friendly_bard",
        "friendly--bard",
        "FrIeNdLy-._.-bArD",
    )
    outputs = list(map(normalize_package_name, inputs))
    assert len(set(outputs)) == 1
