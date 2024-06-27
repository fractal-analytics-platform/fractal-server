import pytest

from fractal_server.tasks.v2.utils import _init_venv_v2
from fractal_server.tasks.v2.utils import get_python_interpreter_v2
from tests.execute_command import execute_command


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


@pytest.mark.parametrize("python_version", [None, "3.10"])
async def test_init_venv(tmp_path, python_version):
    """
    GIVEN a path and a python version
    WHEN _init_venv_v2() is called
    THEN a python venv is initialised at path
    """
    venv_path = tmp_path / "fractal_test"
    venv_path.mkdir(exist_ok=True, parents=True)
    logger_name = "fractal"

    try:
        python_bin = await _init_venv_v2(
            path=venv_path,
            logger_name=logger_name,
            python_version=python_version,
        )
    except ValueError as e:
        pytest.xfail(reason=str(e))

    assert venv_path.exists()
    assert (venv_path / "venv").exists()
    assert (venv_path / "venv/bin/python").exists()
    assert (venv_path / "venv/bin/pip").exists()
    assert python_bin.exists()
    assert python_bin == venv_path / "venv/bin/python"
    if python_version:
        version = await execute_command(f"{python_bin} --version")
        assert python_version in version
