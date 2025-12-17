import subprocess
import tempfile
from pathlib import Path
from zipfile import ZipFile

import pytest


@pytest.fixture(scope="module")
def tmp_dir_module():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture(scope="module")
def gitignored_file():
    path = "fractal_server/tmp_dummy.py"
    Path(path).touch()
    yield path
    Path(path).unlink()


@pytest.fixture(scope="module")
def build_namelist(
    tmp_dir_module: Path,
    gitignored_file: str,
) -> list[str]:
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

    with ZipFile(wheel_file_path, "r") as zip:
        namelist = list(zip.namelist())
    return namelist


def test_include_task_template(build_namelist: list[str]):
    assert (
        "fractal_server/tasks/v2/templates/1_create_venv.sh" in build_namelist
    )


def test_include_alembic_ini(build_namelist: list[str]):
    assert "fractal_server/alembic.ini" in build_namelist


@pytest.mark.xfail(reason="FIXME")
def test_exclude_gitignored_file(
    build_namelist: list[str],
    gitignored_file: str,
):
    assert gitignored_file not in build_namelist


@pytest.mark.xfail(reason="FIXME")
def test_exclude_json_schemas(build_namelist: list[str]):
    assert not [
        name for name in build_namelist if "fractal_server/json_schemas" in name
    ]


@pytest.mark.xfail(reason="FIXME")
def test_exclude_data_migrations_old(build_namelist: list[str]):
    assert "fractal_server/data_migrations/old/2_18_0.py" not in build_namelist


@pytest.mark.xfail(reason="FIXME")
def test_exclude_mako(build_namelist: list[str]):
    assert "fractal_server/migrations/script.py.mako" not in build_namelist
