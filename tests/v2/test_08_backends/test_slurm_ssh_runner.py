import json
from pathlib import Path

import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_ssh.runner import (
    SlurmSSHRunner,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.ssh
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
    fractal_ssh,
    history_mock_for_submit,
    override_settings_factory,
    task_type: str,
    current_py_version: str,
    valid_user_id,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/.venv{current_py_version}/bin/python{current_py_version}"  # noqa
    )

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
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


@pytest.mark.ssh
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
    fractal_ssh,
    history_mock_for_submit,
    override_settings_factory,
    task_type: str,
    current_py_version: str,
    valid_user_id,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/.venv{current_py_version}/bin/python{current_py_version}"  # noqa
    )

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            base_command="false",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
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


@pytest.mark.ssh
@pytest.mark.container
async def test_multisubmit_parallel(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_multisubmit,
    override_settings_factory,
    current_py_version: str,
    valid_user_id,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/.venv{current_py_version}/bin/python{current_py_version}"  # noqa
    )

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
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
            config=get_default_slurm_config(),
            history_unit_ids=history_unit_ids,
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


@pytest.mark.ssh
@pytest.mark.container
async def test_multisubmit_compound(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_multisubmit,
    override_settings_factory,
    current_py_version: str,
    valid_user_id,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/.venv{current_py_version}/bin/python{current_py_version}"  # noqa
    )

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
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
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            list_parameters=ZARR_URLS_AND_PARAMETER,
            list_task_files=list_task_files,
            task_type="compound",
            config=get_default_slurm_config(),
            history_unit_ids=history_unit_ids,
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


@pytest.mark.ssh
@pytest.mark.container
def test_send_many_job_inputs_failure(tmp777_path: Path, fractal_ssh):
    root_dir_local = tmp777_path / "local"
    root_dir_local.mkdir(parents=True)
    with (root_dir_local / "test.json").open("w") as f:
        json.dump({"foo": "bar"}, f, indent=2)

    remote_dir = tmp777_path / "remote"

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=root_dir_local,
        root_dir_remote=remote_dir,
        poll_interval=0,
    ) as runner:
        runner._mkdir_remote_folder(folder=runner.root_dir_remote.as_posix())
        runner._send_many_job_inputs(
            workdir_local=runner.root_dir_local,
            workdir_remote=runner.root_dir_remote,
        )
        runner.fractal_ssh._connection.sftp().stat(
            (runner.root_dir_remote / "test.txt").as_posix()
        )
        # runner.fractal_ssh.close()
        # with pytest.raises(UnexpectedExit):
        #     runner._send_many_job_inputs(
        #         workdir_local=runner.root_dir_local,
        #         workdir_remote=runner.root_dir_remote,
        #     )
