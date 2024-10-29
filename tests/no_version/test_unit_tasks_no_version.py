from pathlib import Path

from fractal_server.tasks.utils import get_absolute_venv_path_v1
from fractal_server.tasks.utils import normalize_package_name


def test_get_absolute_venv_path(tmp_path, override_settings_factory):
    FRACTAL_TASKS_DIR = tmp_path / "TASKS"
    override_settings_factory(FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR)
    absolute_path = tmp_path
    relative_path = Path("somewhere/else/")
    assert get_absolute_venv_path_v1(absolute_path) == absolute_path
    assert get_absolute_venv_path_v1(relative_path) == (
        FRACTAL_TASKS_DIR / relative_path
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
