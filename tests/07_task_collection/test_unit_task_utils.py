from pathlib import Path

from fractal_server.tasks.utils import get_absolute_venv_path


def test_get_absolute_venv_path(tmp_path, override_settings_factory):
    FRACTAL_TASKS_DIR = tmp_path / "TASKS"
    override_settings_factory(FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR)
    absolute_path = tmp_path
    relative_path = Path("somewhere/else/")
    assert get_absolute_venv_path(absolute_path) == absolute_path
    assert get_absolute_venv_path(relative_path) == (
        FRACTAL_TASKS_DIR / relative_path
    )
