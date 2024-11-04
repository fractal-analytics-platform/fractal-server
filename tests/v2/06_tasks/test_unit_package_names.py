import pytest

from fractal_server.tasks.v2.utils_package_names import _parse_wheel_filename
from fractal_server.tasks.v2.utils_package_names import compare_package_names
from fractal_server.tasks.v2.utils_package_names import normalize_package_name


def test_parse_wheel_filename():
    with pytest.raises(
        ValueError,
        match="Input must be a filename, not a full path",
    ):
        _parse_wheel_filename(wheel_filename="/tmp/something.whl")


def test_compare_package_names(caplog):

    compare_package_names(
        pkg_name_pip_show="aaa",
        pkg_name_task_group="aaa",
        logger_name=None,
    )

    caplog.clear()
    compare_package_names(
        pkg_name_pip_show="fractal-tasks-core",
        pkg_name_task_group="FRACTAL____TASKS___CORE",
        logger_name=None,
    )
    assert "Package name mismatc" in caplog.text

    with pytest.raises(ValueError):
        compare_package_names(
            pkg_name_pip_show="something",
            pkg_name_task_group="else",
            logger_name=None,
        )


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
