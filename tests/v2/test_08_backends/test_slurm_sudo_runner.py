import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    SudoSlurmRunner,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.container
@pytest.mark.parametrize(
    "task_type",
    [
        "non_parallel",
        # "compound",
        # "converter_non_parallel",
        # "converter_compound",
    ],
)
async def test_submit_success(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    task_type: str,
    valid_user_id,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    debug(result, exception)
    return  # FIXME
    assert result == {}
    assert exception is None

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    if task_type in ["non_parallel", "converter_non_parallel"]:
        assert unit.status == HistoryUnitStatus.DONE
    else:
        assert unit.status == HistoryUnitStatus.SUBMITTED


@pytest.mark.container
@pytest.mark.parametrize(
    "task_type",
    [
        "non_parallel",
        "compound",
        "converter_non_parallel",
        "converter_compound",
    ],
)
async def test_submit_fail(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_submit,
    task_type: str,
    valid_user_id,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            workflow_task_order="0",
            workflow_task_id=9999,
            task_name="fake-task-name",
            base_command="false",
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(
                tmp777_path,
                component="0",
                is_slurm=True,
            ),
            config=get_default_slurm_config(),
            task_type=task_type,
            user_id=valid_user_id,
        )
    debug(result, exception)
    assert result is None
    assert isinstance(exception, TaskExecutionError)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.skip(reason="Not ready yet - FIXME")
@pytest.mark.container
async def test_multisubmit_parallel(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
):
    def fun(parameters: dict, remote_files: dict):
        zarr_url = parameters["zarr_url"]
        x = parameters["parameter"]
        if x != 3:
            print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
            return 2 * x
        else:
            print(f"Running with {zarr_url=} and {x=}, raising error.")
            raise ValueError("parameter=3 is very very bad")

    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path, component=str(ind), is_slurm=True
                )
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    debug(results)
    debug(exceptions)
    assert results == {
        0: 2,
        1: 4,
        3: 8,
    }
    # assert isinstance(exceptions[2], ValueError) # TaskExecutionError
    assert "very very bad" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        if ind != 2:
            assert unit.status == HistoryUnitStatus.DONE
        else:
            assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit_compound(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
):
    def fun(parameters: dict, remote_files: dict):
        zarr_url = parameters["zarr_url"]
        x = parameters["parameter"]
        if x != 3:
            print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
            return 2 * x
        else:
            print(f"Running with {zarr_url=} and {x=}, raising error.")
            raise ValueError("parameter=3 is very very bad")

    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        list_task_files = [
            get_dummy_task_files(
                tmp777_path,
                component=str(ind),
                is_slurm=True,
            )
            for ind in range(len(ZARR_URLS))
        ]
        # Create task subfolder (in standard usage, this was done during the
        # init phase)
        workdir_local = list_task_files[0].wftask_subfolder_local
        workdir_remote = list_task_files[0].wftask_subfolder_remote
        runner._mkdir_local_folder(workdir_local.as_posix())
        runner._mkdir_remote_folder(folder=workdir_remote.as_posix())
        results, exceptions = runner.multisubmit(
            fun,
            ZARR_URLS_AND_PARAMETER,
            list_task_files=list_task_files,
            task_type="compound",
            history_unit_ids=history_unit_ids,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    debug(results)
    debug(exceptions)
    assert results == {
        0: 2,
        1: 4,
        3: 8,
    }
    # assert isinstance(exceptions[2], ValueError) # TaskExecutionError
    assert "very very bad" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    for _unit_id in history_unit_ids:
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        # `HistoryUnit.status` is not updated from within `runner.multisubmit`,
        # for compound tasks
        assert unit.status == HistoryUnitStatus.SUBMITTED
