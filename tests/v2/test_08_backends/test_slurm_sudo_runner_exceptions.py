import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_sudo.runner import (
    SlurmSudoRunner,
)
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.container
async def test_submit_exception(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    parameters = dict(zarr_urls=ZARR_URLS)

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        runner.jobs = {"0": "fake"}

        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    assert isinstance(exception, JobExecutionError)
    assert "jobs must be empty" in str(exception)

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit_exception_submission(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    """
    Fail because of invalid parameters.
    """

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
            list_parameters=[
                {"non_zarr_url": "something"}
            ],  # invalid parameters
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path,
                    component="0",
                    is_slurm=True,
                )
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    assert results == {}
    for e in exceptions.values():
        assert isinstance(e, ValueError)
        assert "differs from len" in str(e)

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit_exception_fetch_artifacts(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    def fake_fetch_artifacts(*args, **kwargs):
        raise RuntimeError("Error from fake_fetch_artifacts.")

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        runner._fetch_artifacts = fake_fetch_artifacts

        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
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

    assert results == {}
    for e in exceptions.values():
        assert isinstance(e, RuntimeError)
        assert "Error from fake_fetch_artifacts" in str(e)

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit_exception_postprocess_single_task(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_multisubmit,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    def fake_postprocess_single_task(*args, **kwargs):
        raise RuntimeError("Error from fake_postprocess_single_task.")

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    resource, profile = slurm_sudo_resource_profile_objects[:]

    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        runner._postprocess_single_task = fake_postprocess_single_task

        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
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

    assert results == {}
    for e in exceptions.values():
        assert isinstance(e, RuntimeError)
        assert "Error from fake_postprocess_single_task" in str(e)

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        assert unit.status == HistoryUnitStatus.FAILED
