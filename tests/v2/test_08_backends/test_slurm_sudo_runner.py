import shutil

import pytest
from devtools import debug

from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.exceptions import TaskExecutionError
from fractal_server.runner.executors.slurm_sudo.runner import SlurmSudoRunner
from fractal_server.runner.task_files import MULTISUBMIT_PREFIX
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER


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
async def test_submit_success(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    task_type: str,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            task_files=get_dummy_task_files(
                tmp777_path,
                component="0",
                is_slurm=True,
            ),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    assert result is None
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
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        result, exception = runner.submit(
            base_command="false",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
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


@pytest.mark.container
async def test_multisubmit_parallel(
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
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path,
                    component=str(ind),
                    is_slurm=True,
                    # Set a realistic prefix (c.f. `enrich_task_files_multisubmit` function)  # noqa
                    prefix=f"{MULTISUBMIT_PREFIX}-{ind:06d}",
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
    assert results == {key: None for key in range(4)}
    assert exceptions == {}

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        assert unit.status == HistoryUnitStatus.DONE


@pytest.mark.container
async def test_multisubmit_compound(
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
        list_task_files = [
            get_dummy_task_files(
                tmp777_path,
                component=str(ind),
                is_slurm=True,
                # Set a realistic prefix (c.f. `enrich_task_files_multisubmit` function)  # noqa
                prefix=f"{MULTISUBMIT_PREFIX}-{ind:06d}",
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
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=list_task_files,
            task_type="compound",
            history_unit_ids=history_unit_ids,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )

    debug(results)
    debug(exceptions)

    assert results == {key: None for key in range(4)}
    assert exceptions == {}

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


@pytest.mark.container
async def test_multisubmit_parallel_partial_failure(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    testdata_path,
    slurm_sudo_resource_profile_objects,
):
    raw_script_path = testdata_path / "script_for_selective_failure.py"
    script_path = tmp777_path / "script_for_selective_failure.py"

    shutil.copy(raw_script_path, script_path)

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        results, exceptions = runner.multisubmit(
            base_command=f"python3 {script_path.as_posix()}",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path,
                    component=str(ind),
                    is_slurm=True,
                    # Set a realistic prefix (c.f. `enrich_task_files_multisubmit` function)  # noqa
                    prefix=f"{MULTISUBMIT_PREFIX}-{ind:06d}",
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
    assert results == {key: None for key in range(1, 4)}
    assert isinstance(exceptions[0], TaskExecutionError)
    assert "Bad result" in str(exceptions[0])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        if ind == 0:
            assert unit.status == HistoryUnitStatus.FAILED
        else:
            assert unit.status == HistoryUnitStatus.DONE
