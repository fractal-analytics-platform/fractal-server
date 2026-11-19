"""
Run `SlurmRestRunner` against the real `slurmrestd` of the SLURM docker
container (Slurm 23.11, API `v0.0.40`, JWT auth, behind a TLS proxy, see
`tests/fixtures_slurm_rest.py`). Tokens are signed with the test key of
the container, and set through the `FRACTAL_SLURM_REST_TOKEN` development
setting.

Inherited behaviour (task failures, compound tasks, fetching and
postprocessing outputs, ...) is the same as for `SlurmSSHRunner`, and is
covered by the SLURM/SSH and SLURM/sudo runner tests.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.runner.task_files import TaskFiles
from tests.fixtures_slurm_rest import SLURM_REST_API_VERSION
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER

# Maximum wait for a SLURM job state, in seconds
MAX_WAIT = 120


def _get_runner(tmp777_path, fractal_ssh, res, prof, job_id):
    return SlurmRestRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=res,
        profile=prof,
        fractal_job_id=job_id,
        resource_id=res.id,
        user_id=1,
    )


def _submit(runner, *, base_command, task_files, history_mock, user_id):
    _, history_unit_id, wftask_id = history_mock
    return runner.submit(
        base_command=base_command,
        workflow_task_order=0,
        workflow_task_id=wftask_id,
        task_name="fake-task-name",
        parameters=dict(zarr_urls=ZARR_URLS),
        task_files=task_files,
        task_type="non_parallel",
        history_unit_id=history_unit_id,
        config=get_default_slurm_config(),
        user_id=user_id,
    )


def _submitted_job(task_files: TaskFiles) -> dict:
    """
    JSON job description of the (single) submitted SLURM job, from the
    request saved locally next to the submission script.
    """
    request_files = list(
        task_files.wftask_subfolder_local.glob("*-slurm-submit.json")
    )
    assert len(request_files) == 1
    return json.loads(request_files[0].read_text())["job"]


@pytest.fixture
def reset_permissions(run_in_container, tmp777_path):
    """
    Make remote job files removable at the end of the test (they may belong
    to a SLURM user other than the SSH user).
    """
    yield
    run_in_container(f"chmod -R 777 {tmp777_path / 'user'}")


@pytest.mark.ssh
@pytest.mark.container
@pytest.mark.parametrize("slurm_user", ["test01", "test02"])
async def test_rest_submit(
    slurm_user,
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_submit,
    valid_user_id,
    slurm_rest_resource_profile_db: tuple[Resource, Profile],
    slurm_rest_token,
    override_settings_factory,
    reset_permissions,
    fractal_job_id_mock,
):
    """
    Submit a job through `slurmrestd`, as the SSH user (`test01`) or as
    another SLURM user (`test02`, in group `test01`, as with SSO tokens and a
    shared SSH account).
    """
    res, prof = slurm_rest_resource_profile_db[:]
    assert prof.username == "test01"
    override_settings_factory(
        FRACTAL_SLURM_REST_TOKEN=slurm_rest_token(slurm_user)
    )
    task_files = get_dummy_task_files(tmp777_path, component="0", is_slurm=True)

    with _get_runner(
        tmp777_path, fractal_ssh, res, prof, fractal_job_id_mock
    ) as runner:
        assert runner.slurm_username == slurm_user
        result, exception = _submit(
            runner,
            base_command="true",
            task_files=task_files,
            history_mock=history_mock_for_submit,
            user_id=valid_user_id,
        )
    assert result is None
    assert exception is None
    _, history_unit_id, _ = history_mock_for_submit
    unit = await db.get(HistoryUnit, history_unit_id)
    assert unit.status == HistoryUnitStatus.DONE

    job = _submitted_job(task_files)
    # Same stdout path as `sbatch` over SSH
    # (`<subfolder>/<prefix>-slurm-%j.out`)
    assert job["standard_output"] == (
        f"{task_files.wftask_subfolder_remote.as_posix()}/"
        f"{task_files.prefix}-slurm-%j.out"
    )
    assert job["current_working_directory"] == (
        task_files.wftask_subfolder_remote.as_posix()
    )
    # Job environment of the SLURM user, not of the SSH user
    assert f"USER={slurm_user}" in job["environment"]
    assert f"HOME=/home/{slurm_user}" in job["environment"]
    assert "SLURM_CPUS_PER_TASK=1" in job["environment"]

    # The args file was written remotely by the job itself (no SSH upload),
    # as the SLURM user, and it is readable by the SSH user
    args_file = task_files.args_file_remote
    assert fractal_ssh.read_remote_json_file(args_file) == dict(
        zarr_urls=ZARR_URLS
    )
    owner = fractal_ssh.run_command(cmd=f"stat -c %U {args_file}").strip()
    assert owner == slurm_user


@pytest.mark.ssh
@pytest.mark.container
async def test_rest_multisubmit_parallel(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_multisubmit,
    slurm_rest_resource_profile_db: tuple[Resource, Profile],
    slurm_rest_token,
    override_settings_factory,
    reset_permissions,
    valid_user_id,
    fractal_job_id_mock,
):
    res, prof = slurm_rest_resource_profile_db[:]
    override_settings_factory(FRACTAL_SLURM_REST_TOKEN=slurm_rest_token())
    _, history_unit_ids, wftask_id = history_mock_for_multisubmit

    with _get_runner(
        tmp777_path, fractal_ssh, res, prof, fractal_job_id_mock
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
                    prefix=f"{MULTISUBMIT_PREFIX}-{ind:06d}",
                )
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            config=get_default_slurm_config(),
            history_unit_ids=history_unit_ids,
            user_id=valid_user_id,
        )
    assert results == {key: None for key in range(4)}
    assert exceptions == {}
    for unit_id in history_unit_ids:
        unit = await db.get(HistoryUnit, unit_id)
        assert unit.status == HistoryUnitStatus.DONE


@pytest.mark.ssh
@pytest.mark.container
async def test_rest_submit_shutdown(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_submit,
    slurm_rest_resource_profile_db: tuple[Resource, Profile],
    slurm_rest_token,
    override_settings_factory,
    run_in_container,
    reset_permissions,
    valid_user_id,
    fractal_job_id_mock,
):
    """
    A shutdown while the SLURM job runs cancels it through `slurmrestd`.
    """
    res, prof = slurm_rest_resource_profile_db[:]
    override_settings_factory(FRACTAL_SLURM_REST_TOKEN=slurm_rest_token())
    slurm_job_ids = []

    with _get_runner(
        tmp777_path, fractal_ssh, res, prof, fractal_job_id_mock
    ) as runner:

        def shutdown_when_running(fut_submit):
            deadline = time.monotonic() + MAX_WAIT
            while time.monotonic() < deadline:
                if fut_submit.done():
                    raise RuntimeError(
                        "`submit` returned before the SLURM job was RUNNING: "
                        f"{fut_submit.result()}"
                    )
                if runner.jobs:
                    job_id = list(runner.jobs)[0]
                    state = run_in_container(
                        f"squeue --noheader --format=%T --jobs={job_id}"
                    ).stdout.strip()
                    if state == "RUNNING":
                        slurm_job_ids.append(job_id)
                        runner.shutdown_file.touch()
                        return
                time.sleep(0.2)
            runner.shutdown_file.touch()
            raise TimeoutError("SLURM job not RUNNING.")

        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_submit = executor.submit(
                _submit,
                runner,
                # Not `sleep 1000`: the runner appends `--args-json` and
                # `--out-json` options to the command
                base_command="bash -c 'sleep 1000'",
                task_files=get_dummy_task_files(
                    tmp777_path, component="0", is_slurm=True
                ),
                history_mock=history_mock_for_submit,
                user_id=valid_user_id,
            )
            fut_shutdown = executor.submit(shutdown_when_running, fut_submit)
            fut_shutdown.result()
            result, exception = fut_submit.result()

    assert isinstance(exception, JobExecutionError)
    assert "shutdown" in str(exception)
    _, history_unit_id, _ = history_mock_for_submit
    unit = await db.get(HistoryUnit, history_unit_id)
    assert unit.status == HistoryUnitStatus.FAILED

    # The SLURM job was cancelled
    (job_id,) = slurm_job_ids
    deadline = time.monotonic() + MAX_WAIT
    while "JobState=CANCELLED" not in (
        output := run_in_container(f"scontrol show job {job_id}").stdout
    ):
        assert time.monotonic() < deadline, output
        time.sleep(0.5)


@pytest.mark.ssh
@pytest.mark.container
def test_rest_api_version_not_served(
    tmp777_path,
    fractal_ssh,
    slurm_rest_resource_profile_objects: tuple[Resource, Profile],
    slurm_rest_token,
    override_settings_factory,
):
    """
    If the configured API version is not served, the runner fails at start
    and lists the supported versions that `slurmrestd` serves.
    """
    res, prof = slurm_rest_resource_profile_objects[:]
    override_settings_factory(FRACTAL_SLURM_REST_TOKEN=slurm_rest_token())
    rest = res.jobs_runner_config["rest"] | dict(api_version="v0.0.44")
    res.jobs_runner_config = res.jobs_runner_config | dict(rest=rest)

    with pytest.raises(JobExecutionError) as exc_info:
        _get_runner(tmp777_path, fractal_ssh, res, prof, job_id=1)
    message = exc_info.value.assemble_error()
    assert "does not serve API version 'v0.0.44'" in message
    assert f"served by slurmrestd: ['{SLURM_REST_API_VERSION}']" in message
