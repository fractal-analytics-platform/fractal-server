from pathlib import Path

import pytest

from fractal_server.app.runner.executors.slurm.utils_executors import (
    get_pickle_file_path,
)
from fractal_server.app.runner.executors.slurm.utils_executors import (
    get_slurm_file_path,
)
from fractal_server.app.runner.executors.slurm.utils_executors import (
    get_slurm_script_file_path,
)
from tests.v2._aux_runner import (
    get_default_task_files,
)


def test_get_default_task_files():
    local_dir = Path("/tmp/local_workflow")
    remote_dir = Path("/tmp/remote_workflow")

    result = get_default_task_files(
        workflow_dir_local=local_dir, workflow_dir_remote=remote_dir
    )

    assert result.workflow_dir_local == local_dir
    assert result.workflow_dir_remote == remote_dir
    assert result.task_order is None
    assert result.task_name == "name"


@pytest.mark.parametrize(
    "in_or_out, expected_suffix",
    [
        ("in", "_in_test.pickle"),
        ("out", "_out_test.pickle"),
    ],
)
def test_get_pickle_file_path(tmp_path, in_or_out, expected_suffix):
    workflow_dir = tmp_path
    subfolder = "subfolder"
    arg = "test"
    prefix = "prefix"

    (workflow_dir / subfolder).mkdir()

    result = get_pickle_file_path(
        arg=arg,
        workflow_dir=workflow_dir,
        subfolder_name=subfolder,
        in_or_out=in_or_out,
        prefix=prefix,
    )

    assert result == workflow_dir / subfolder / f"{prefix}{expected_suffix}"


def test_get_pickle_file_path_invalid(tmp_path):
    workflow_dir = tmp_path
    subfolder = "subfolder"
    arg = "test"
    prefix = "prefix"

    (workflow_dir / subfolder).mkdir()

    with pytest.raises(ValueError):
        get_pickle_file_path(
            arg=arg,
            workflow_dir=workflow_dir,
            subfolder_name=subfolder,
            in_or_out="invalid",
            prefix=prefix,
        )


def test_get_slurm_script_file_path(tmp_path):
    workflow_dir = tmp_path
    subfolder = "subfolder"
    prefix = "custom"

    (workflow_dir / subfolder).mkdir()

    result = get_slurm_script_file_path(
        workflow_dir=workflow_dir,
        subfolder_name=subfolder,
        prefix=prefix,
    )

    assert result == workflow_dir / subfolder / f"{prefix}_slurm_submit.sbatch"


@pytest.mark.parametrize(
    "out_or_err, prefix, expected_suffix",
    [
        ("out", "custom_stdout", "_slurm_%j.out"),
        ("err", "custom_stderr", "_slurm_%j.err"),
    ],
)
def test_get_slurm_file_path(tmp_path, out_or_err, prefix, expected_suffix):
    workflow_dir = tmp_path
    subfolder = "subfolder"

    (workflow_dir / subfolder).mkdir()

    result = get_slurm_file_path(
        workflow_dir=workflow_dir,
        subfolder_name=subfolder,
        arg="%j",
        out_or_err=out_or_err,
        prefix=prefix,
    )

    assert result == workflow_dir / subfolder / f"{prefix}{expected_suffix}"


def test_get_slurm_file_path_invalid_out_or_err(tmp_path):
    workflow_dir = tmp_path
    subfolder = "subfolder"

    with pytest.raises(ValueError, match="unexpected value out_or_err"):
        get_slurm_file_path(
            workflow_dir=workflow_dir,
            subfolder_name=subfolder,
            arg="%j",
            out_or_err="invalid",
            prefix="prefix",
        )
