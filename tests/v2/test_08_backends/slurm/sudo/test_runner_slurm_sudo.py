import time
from pathlib import Path

import pytest
from devtools import debug

from ...aux_unit_runner import *  # noqa
from ...aux_unit_runner import ZARR_URLS
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    RunnerSlurmSudo,
)
from fractal_server.app.runner.task_files import TaskFiles
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config


def get_dummy_task_files(root_path: Path) -> TaskFiles:
    return TaskFiles(
        root_dir_local=root_path / "server",
        root_dir_remote=root_path / "user",
        task_name="name",
        task_order=0,
    )


@pytest.mark.container
async def test_submit_success(
    tmp777_path,
    monkey_slurm,
):
    def do_nothing(parameters: dict, **kwargs) -> int:
        return 42

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            history_item_id=None,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type="non_parallel",
        )
    debug(result, exception)
    assert result == 42
    assert exception is None


@pytest.mark.container
async def test_submit_fail(
    tmp777_path,
    monkey_slurm,
):
    ERROR_MSG = "very nice error"

    def raise_ValueError(parameters: dict, **kwargs):
        raise ValueError(ERROR_MSG)

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            history_item_id=None,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type="non_parallel",
        )

    assert result is None
    assert isinstance(exception, TaskExecutionError)
    assert ERROR_MSG in str(exception)


@pytest.mark.container
async def test_multisubmit(
    tmp777_path,
    monkey_slurm,
):
    def fun(parameters: dict, **kwargs):
        zarr_url = parameters["zarr_url"]
        x = parameters["parameter"]
        if x != 3:
            print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
            time.sleep(1)
            return 2 * x
        else:
            print(f"Running with {zarr_url=} and {x=}, raising error.")
            time.sleep(1)
            raise ValueError("parameter=3 is very very bad")

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            [
                {
                    "zarr_url": "a",
                    "parameter": 1,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000000",
                },
                {
                    "zarr_url": "b",
                    "parameter": 2,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000001",
                },
                {
                    "zarr_url": "c",
                    "parameter": 3,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000002",
                },
                {
                    "zarr_url": "d",
                    "parameter": 4,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000003",
                },
            ],
            history_item_id=None,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type="parallel",
        )
        debug(results)
        debug(exceptions)
