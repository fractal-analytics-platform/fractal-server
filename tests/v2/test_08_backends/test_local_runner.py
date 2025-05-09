import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import get_default_local_backend_config
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.local.runner import LocalRunner
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


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
    history_mock_for_submit,
    tmp_path,
    task_type: str,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with LocalRunner(tmp_path) as runner:
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            task_files=get_dummy_task_files(tmp_path, component="0"),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_local_backend_config(),
            user_id=None,
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
    history_mock_for_submit,
    tmp_path,
    task_type: str,
):

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with LocalRunner(root_dir_local=tmp_path) as runner:
        result, exception = runner.submit(
            base_command="false",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            task_files=get_dummy_task_files(tmp_path, component="0"),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_local_backend_config(),
            user_id=None,
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


async def test_submit_inner_failure(
    db,
    history_mock_for_submit,
    tmp_path,
    monkeypatch,
):
    ERROR_MSG = "very nice error"

    def mock_validate_params(*args, **kwargs):
        raise ValueError(ERROR_MSG)

    from fractal_server.app.runner.executors.local.runner import BaseRunner

    monkeypatch.setattr(
        BaseRunner, "validate_submit_parameters", mock_validate_params
    )

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=dict(zarr_urls=ZARR_URLS),
            task_files=get_dummy_task_files(tmp_path, component="0"),
            task_type="parallel",
            history_unit_id=history_unit_id,
            config=get_default_local_backend_config(),
            user_id=None,
        )
    debug(result, exception)
    assert result is None
    assert isinstance(exception, TaskExecutionError)
    assert ERROR_MSG in str(exception)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


async def test_multisubmit_parallel(
    tmp_path,
    db,
    history_mock_for_multisubmit,
):

    # FIXME THESE ARE ALL SUCCESSFUL TASKS, CAN WE MAKE A PARTIALLY-FAILED ONE?

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit
    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
            user_id=None,
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


async def test_multisubmit_compound(
    tmp_path,
    db,
    history_mock_for_multisubmit,
):
    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="compound",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
            user_id=None,
        )
    debug(results)
    debug(exceptions)
    # FIXME ADD ASSERTIONS

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


@pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 1000])
async def test_multisubmit_in_chunks(
    tmp_path,
    db,
    history_mock_for_multisubmit,
    parallel_tasks_per_job,
):
    config = get_default_local_backend_config()
    config.parallel_tasks_per_job = parallel_tasks_per_job

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=config,
            user_id=None,
        )
    assert results == {key: None for key in range(4)}
    assert exceptions == {}

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        assert unit.status == HistoryUnitStatus.DONE


async def test_multisubmit_parallel_fail(
    tmp_path,
    db,
    history_mock_for_multisubmit,
    monkeypatch,
):
    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    def _fake_submit(*args, **kwargs):
        raise ValueError("Error")

    from fractal_server.app.runner.executors.local.runner import (
        ThreadPoolExecutor,
    )

    monkeypatch.setattr(ThreadPoolExecutor, "submit", _fake_submit)

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
            user_id=None,
        )
    debug(results)
    debug(exceptions)
    for exception in exceptions.values():
        assert isinstance(exception, TaskExecutionError)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        assert unit.status == HistoryUnitStatus.FAILED


async def test_multisubmit_inner_failure(
    db,
    history_mock_for_multisubmit,
    tmp_path,
    monkeypatch,
):
    ERROR_MSG = "very nice error"

    def mock_validate_params(*args, **kwargs):
        raise ValueError(ERROR_MSG)

    from fractal_server.app.runner.executors.local.runner import BaseRunner

    monkeypatch.setattr(
        BaseRunner, "validate_multisubmit_parameters", mock_validate_params
    )
    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
            user_id=None,
        )

    debug(results)
    debug(exceptions)
    for exception in exceptions.values():
        assert isinstance(exception, TaskExecutionError)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        assert unit.status == HistoryUnitStatus.FAILED
