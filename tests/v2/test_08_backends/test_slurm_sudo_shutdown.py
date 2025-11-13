import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_sudo.runner import (
    SlurmSudoRunner,
)
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


def _run_squeue():
    res = subprocess.run(
        [
            "squeue",
            "--noheader",
            "--format='%i %T'",
            "--states=all",
        ],
        encoding="utf-8",
        check=True,
        capture_output=True,
    )
    return res.stdout


def shutdown_thread(
    shutdown_file: Path,
    initial_grace_time: float = 0.0,
):
    debug("[shutdown_thread] START")
    time.sleep(initial_grace_time)
    squeue_output = _run_squeue()
    while True:
        squeue_output = _run_squeue()
        debug(squeue_output)
        if "RUNNING" in squeue_output:
            debug("[shutdown_thread] Found job RUNNING, break.")
            break
        else:
            debug("[shutdown_thread] Wait longer.")
            time.sleep(0.01)
    debug(f"[shutdown_thread] Now create {shutdown_file}")
    shutdown_file.touch()
    debug("[shutdown_thread] END")


@pytest.mark.container
async def test_submit_shutdown(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:

        def main_thread():
            debug("[main_thread] START")
            result, exception = runner.submit(
                base_command="bash -c 'sleep 1000'",
                workflow_task_order=0,
                workflow_task_id=wftask_id,
                task_name="fake-task-name",
                parameters=dict(zarr_urls=ZARR_URLS),
                task_files=get_dummy_task_files(
                    tmp777_path, component="", is_slurm=True
                ),
                task_type="non_parallel",
                history_unit_id=history_unit_id,
                config=get_default_slurm_config(),
                user_id=valid_user_id,
            )
            debug("[main_thread] END")
            return result, exception

        with ThreadPoolExecutor(max_workers=2) as executor:
            fut1 = executor.submit(main_thread)
            fut2 = executor.submit(shutdown_thread, runner.shutdown_file)
            fut2.result()
            result, exception = fut1.result()
        debug(result, exception)
        assert isinstance(exception, JobExecutionError)
        assert "shutdown" in str(exception)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit_shutdown(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:

        def main_thread():
            debug("[main_thread] START")
            results, exceptions = runner.multisubmit(
                base_command="bash -c 'sleep 1000'",
                workflow_task_order=0,
                workflow_task_id=wftask_id,
                task_name="fake-task-name",
                list_parameters=ZARR_URLS_AND_PARAMETER,
                list_task_files=[
                    get_dummy_task_files(
                        tmp777_path,
                        component=str(ind),
                        is_slurm=True,
                    )
                    for ind in range(len(ZARR_URLS))
                ],
                task_type="parallel",
                config=get_default_slurm_config(),
                history_unit_ids=history_unit_ids,
                user_id=valid_user_id,
            )
            return results, exceptions

        with ThreadPoolExecutor(max_workers=2) as executor:
            fut1 = executor.submit(main_thread)
            fut2 = executor.submit(
                shutdown_thread,
                runner.shutdown_file,
                initial_grace_time=0.5,
            )
            fut2.result()
            results, exceptions = fut1.result()
        debug(results, exceptions)
        assert isinstance(exceptions[2], JobExecutionError)
        assert "shutdown" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # Check that `HistoryUnit.status` is updated from within
    # `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_shutdown_before_submit(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        # Write shudown file
        runner.shutdown_file.touch()

        # Submit
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=dict(zarr_urls=ZARR_URLS),
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
        debug(result)
        debug(exception)
        assert "shutdown" in str(exception)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_shutdown_before_multisubmit(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        # Write shutdown file right away
        runner.shutdown_file.touch()

        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=[
                {
                    "zarr_url": "a",
                    "parameter": 1,
                },
                {
                    "zarr_url": "b",
                    "parameter": 2,
                },
                {
                    "zarr_url": "c",
                    "parameter": 3,
                },
                {
                    "zarr_url": "d",
                    "parameter": 4,
                },
            ],
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path,
                    component=str(ind),
                    is_slurm=True,
                )
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            config=get_default_slurm_config(),
            history_unit_ids=history_unit_ids,
            user_id=valid_user_id,
        )
        debug(results, exceptions)
        assert results == {}
        for exception in exceptions.values():
            assert isinstance(exception, JobExecutionError)
            assert "shutdown" in str(exception)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        assert unit.status == HistoryUnitStatus.FAILED
