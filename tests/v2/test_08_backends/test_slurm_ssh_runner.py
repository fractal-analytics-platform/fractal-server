import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum
from fractal_server.runner.exceptions import TaskExecutionError
from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.runner.v2.runner import _add_pre_submission_cmd
from fractal_server.ssh._fabric import FractalSSH
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions] {remote_folder=}")
    fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


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
    task_type: str,
    valid_user_id,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
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

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)


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
    task_type: str,
    valid_user_id,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
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

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)


@pytest.mark.ssh
@pytest.mark.container
@pytest.mark.parametrize(
    "origin,expected_status",
    [
        (TaskGroupOriginEnum.APPTAINER, HistoryUnitStatus.DONE),
        (TaskGroupOriginEnum.PYPI, HistoryUnitStatus.FAILED),
    ],
)
async def test_submit_with_pre_submission_cmd(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_submit,
    valid_user_id,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
    origin: TaskGroupOriginEnum,
    expected_status: HistoryUnitStatus,
):
    """
    The task only succeeds if `pre_submission_cmd` ran right before `sbatch`
    (so that the exported variable propagates to the SLURM job), which only
    happens for `apptainer` task groups.
    """
    res, prof = slurm_ssh_resource_profile_db[:]
    _, history_unit_id, wftask_id = history_mock_for_submit

    # Stand-in for `module load apptainer`, with a visible side effect. It
    # also writes to stdout, which must not break the parsing of the SLURM
    # job ID.
    pre_submission_cmd = (
        "echo 'some output'\nexport FRACTAL_TEST_VAR=expected-value"
    )
    check_script = (tmp777_path / "check_env.sh").as_posix()
    fractal_ssh.write_remote_file(
        path=check_script,
        content='#!/bin/sh\ntest "$FRACTAL_TEST_VAR" = expected-value\n',
    )

    get_runner_config = _add_pre_submission_cmd(
        get_runner_config=lambda **kwargs: get_default_slurm_config(),
        task_group_origin=origin,
        resource=Resource(
            tasks_apptainer_config=dict(
                pixi_version="0.1",
                pre_submission_cmd=pre_submission_cmd,
            )
        ),
    )

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
    ) as runner:
        runner.submit(
            base_command=f"sh {check_script}",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=dict(zarr_urls=ZARR_URLS),
            task_files=get_dummy_task_files(
                tmp777_path,
                component="0",
                is_slurm=True,
            ),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
            config=get_runner_config(),
            user_id=valid_user_id,
        )

    unit = await db.get(HistoryUnit, history_unit_id)
    assert unit.status == expected_status

    # `pre_submission_cmd` is in the wrapper script (only for `apptainer`),
    # and never in the SLURM submission script
    remote_dir = tmp777_path / "user"
    wrappers = list(remote_dir.rglob("*-slurm-submit-wrapper.sh"))
    submission_scripts = list(remote_dir.rglob("*-slurm-submit.sh"))
    assert len(submission_scripts) == 1
    assert pre_submission_cmd not in submission_scripts[0].read_text()
    if origin == TaskGroupOriginEnum.APPTAINER:
        assert len(wrappers) == 1
        assert pre_submission_cmd in wrappers[0].read_text()
    else:
        assert wrappers == []

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)


@pytest.mark.ssh
@pytest.mark.container
async def test_multisubmit_parallel(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_multisubmit,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    valid_user_id,
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
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

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)


@pytest.mark.ssh
@pytest.mark.container
async def test_multisubmit_compound(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_multisubmit,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    valid_user_id,
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]

    history_run_id, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
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

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)


@pytest.mark.ssh
@pytest.mark.container
def test_send_many_job_inputs_failure(
    tmp777_path: Path,
    fractal_ssh,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
):
    root_dir_local = tmp777_path / "server"
    root_dir_local.mkdir(parents=True)
    root_dir_remote = tmp777_path / "user"
    dummy_file = root_dir_local / "foo.txt"
    dummy_file.touch()
    res, prof = slurm_ssh_resource_profile_db[:]

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_remote,
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=fractal_job_id_mock,
        resource_id=res.id,
    ) as runner:
        # Set connection to None, so that all SSH-related `fractal_ssh`
        # methods will fail
        runner.fractal_ssh = FractalSSH(connection=None)

        with pytest.raises(
            AttributeError,
            match="'NoneType' object has no attribute 'sftp'",
        ):
            runner._send_many_job_inputs(
                workdir_local=runner.root_dir_local,
                workdir_remote=runner.root_dir_remote,
            )

    tar_path_local = root_dir_local.with_suffix(".tar.gz")
    assert not tar_path_local.exists()
    assert dummy_file.exists()

    _reset_permissions((tmp777_path / "user").as_posix(), fractal_ssh)
