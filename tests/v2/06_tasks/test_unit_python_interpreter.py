import pytest

from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter_v2,
)


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
        get_python_interpreter_v2(python_version=None)
    assert "Invalid python_version=None" in str(e.value)
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(python_version=123)
    assert "Invalid python_version=" in str(e.value)

    # Failures for requiring missing Python version
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(python_version="3.10")
    assert "but FRACTAL_TASKS_PYTHON_3_10=None" in str(e.value)
    with pytest.raises(ValueError) as e:
        get_python_interpreter_v2(python_version="3.11")
    assert "but FRACTAL_TASKS_PYTHON_3_11=None" in str(e.value)

    # Success
    python39 = get_python_interpreter_v2("3.9")
    assert python39 == MOCK_PYTHON_3_9
    python312 = get_python_interpreter_v2("3.12")
    assert python312 == MOCK_PYTHON_3_12
