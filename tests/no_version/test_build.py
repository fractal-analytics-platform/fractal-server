import subprocess
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest


@pytest.fixture(scope="module")
def tmp_dir_module():
    """
    Module-scoped temporary directory.

    Set `delete=False` if you need to manually debug the `uv build` artifacts.
    """
    with tempfile.TemporaryDirectory(delete=True) as tmpdirname:
        yield tmpdirname


@pytest.fixture(scope="module")
def dist_files(tmp_dir_module: Path):
    """
    Run `uv build` and list the files in the wheel.
    """

    # File matching `tmp*`, which should *not* be included in the wheel
    path = "fractal_server/tmp_dummy.py"
    Path(path).touch()

    subprocess.run(
        [
            "uv",
            "build",
            "--out-dir",
            tmp_dir_module,
        ],
        capture_output=True,
        encoding="utf-8",
        check=True,
    )
    wheel_file_path = next(Path(tmp_dir_module).glob("*.whl"))
    print(f"{wheel_file_path=}")

    with ZipFile(wheel_file_path, "r") as zip:
        namelist = list(zip.namelist())

    yield namelist

    Path(path).unlink()


def test_include_task_template(dist_files: list[str]):
    assert "fractal_server/tasks/v2/templates/1_create_venv.sh" in dist_files


def test_include_alembic_ini(dist_files: list[str]):
    assert "fractal_server/alembic.ini" in dist_files


def test_exclude_tmp_(dist_files: list[str]):
    assert not [name for name in dist_files if "tmp" in name]


def test_exclude_json_schemas(dist_files: list[str]):
    assert not [
        name for name in dist_files if "fractal_server/json_schemas" in name
    ]


def test_exclude_data_migrations_old(dist_files: list[str]):
    assert "fractal_server/data_migrations/old" not in dist_files
    assert "fractal_server/data_migrations/old/2_18_0.py" not in dist_files


def test_exclude_mako(dist_files: list[str]):
    assert "fractal_server/migrations/script.py.mako" not in dist_files
