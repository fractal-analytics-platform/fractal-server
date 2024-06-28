from pathlib import Path

import pytest

from fractal_server.tasks.utils import _normalize_package_name
from fractal_server.tasks.utils import get_absolute_venv_path
from fractal_server.tasks.v2.utils import get_python_interpreter_v2


async def test_get_python_interpreter_v2(
    override_settings_factory,
    tmp_path,
):

    MOCK_PYTHON_3_9 = (tmp_path / "python3.9").as_posix()
    MOCK_PYTHON_3_12 = (tmp_path / "python3.12").as_posix()
    override_settings_factory(
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION="3.12",
        FRACTAL_TASKS_PYTHON_3_9=MOCK_PYTHON_3_9,
        FRACTAL_TASKS_PYTHON_3_10=None,
        FRACTAL_TASKS_PYTHON_3_11=None,
        FRACTAL_TASKS_PYTHON_3_12=MOCK_PYTHON_3_12,
    )

    # Failures for invalid versions
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(version=None)
    assert "Invalid version=None" in str(e.value)
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(version=123)
    assert "Invalid version=" in str(e.value)

    # Failures for requiring missing Python version
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(version="3.10")
    assert "but FRACTAL_TASKS_PYTHON_3_10=None" in str(e.value)
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(version="3.11")
    assert "but FRACTAL_TASKS_PYTHON_3_11=None" in str(e.value)

    # Success
    python39 = get_python_interpreter_v2("3.9")
    assert python39 == MOCK_PYTHON_3_9
    python312 = get_python_interpreter_v2("3.12")
    assert python312 == MOCK_PYTHON_3_12


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
    outputs = list(map(_normalize_package_name, inputs))
    assert len(set(outputs)) == 1


def test_get_absolute_venv_path(tmp_path, override_settings_factory):
    FRACTAL_TASKS_DIR = tmp_path / "TASKS"
    override_settings_factory(FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR)
    absolute_path = tmp_path
    relative_path = Path("somewhere/else/")
    assert get_absolute_venv_path(absolute_path) == absolute_path
    assert get_absolute_venv_path(relative_path) == (
        FRACTAL_TASKS_DIR / relative_path
    )
