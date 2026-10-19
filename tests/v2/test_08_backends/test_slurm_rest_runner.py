"""
Run `SlurmRestRunner` against the dockerized SLURM cluster, which has no
`slurmrestd`: the REST client is replaced by a fake that submits the REST
script through `sbatch` over SSH. This covers everything except HTTP (in
particular: inputs embedded in the script, no SSH upload).
"""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.executors.slurm_rest import runner as runner_module
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.ssh._fabric import FractalSSH
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER


class SSHFakeRestClient:
    base_url = "fake://slurmrestd"

    def __init__(self, fractal_ssh: FractalSSH, script_dir: Path):
        self.fractal_ssh = fractal_ssh
        self.script_dir = script_dir
        self.submitted: list[dict] = []
        self.cancelled: list[str] = []

    def ping(self) -> None:
        pass

    def submit_job(self, request_body: dict) -> int:
        self.submitted.append(request_body)
        path = (self.script_dir / f"rest-{uuid4().hex}.sh").as_posix()
        self.fractal_ssh.write_remote_file(
            path=path, content=request_body["script"]
        )
        stdout = self.fractal_ssh.run_command(cmd=f"sbatch --parsable {path}")
        return int(stdout.strip())

    def get_job_states(self) -> dict[str, str]:
        stdout = self.fractal_ssh.run_command(
            cmd="squeue --noheader --format='%i %T' --states=all"
        )
        return dict(line.split() for line in stdout.splitlines())

    def cancel_job(self, job_id: str) -> None:
        self.cancelled.append(job_id)
        self.fractal_ssh.run_command(cmd=f"scancel {job_id}")


class FakeTokenProvider:
    def get_token(self, force_refresh: bool = False) -> str:
        return "x"


@pytest.fixture
def fake_rest(monkeypatch, fractal_ssh, tmp777_path):
    fake = SSHFakeRestClient(fractal_ssh, tmp777_path / "user")
    monkeypatch.setattr(runner_module, "SlurmRestClient", lambda **_: fake)
    monkeypatch.setattr(
        runner_module, "get_token_provider", lambda **_: FakeTokenProvider()
    )
    return fake


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


@pytest.mark.ssh
@pytest.mark.container
async def test_rest_submit(
    db,
    tmp777_path,
    fractal_ssh,
    fake_rest,
    history_mock_for_submit,
    valid_user_id,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]
    _, history_unit_id, wftask_id = history_mock_for_submit

    with _get_runner(
        tmp777_path, fractal_ssh, res, prof, fractal_job_id_mock
    ) as runner:
        task_files = get_dummy_task_files(
            tmp777_path, component="0", is_slurm=True
        )
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=dict(zarr_urls=ZARR_URLS),
            task_files=task_files,
            task_type="non_parallel",
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    assert result is None
    assert exception is None
    unit = await db.get(HistoryUnit, history_unit_id)
    assert unit.status == HistoryUnitStatus.DONE

    assert len(fake_rest.submitted) == 1
    job = fake_rest.submitted[0]["job"]
    # Resume identifies SLURM jobs by this path, see `parse_stdout_path`
    assert job["standard_output"] == (
        f"{task_files.wftask_subfolder_remote.as_posix()}/"
        f"{task_files.prefix}-slurm-%j.out"
    )
    assert "SLURM_CPUS_PER_TASK=1" in job["environment"]
    assert any(env.startswith("HOME=/") for env in job["environment"])
    assert job["current_working_directory"] == (
        task_files.wftask_subfolder_remote.as_posix()
    )
    # Request is saved locally, next to the submission script
    request_files = list(
        task_files.wftask_subfolder_local.glob("*-slurm-submit.json")
    )
    assert len(request_files) == 1
    assert json.loads(request_files[0].read_text())["job"] == job
    # Args file was written remotely by the job itself (no SSH upload)
    remote_args = fractal_ssh.read_remote_json_file(task_files.args_file_remote)
    assert remote_args == dict(zarr_urls=ZARR_URLS)

    fractal_ssh.run_command(cmd=f"chmod -R 777 {tmp777_path / 'user'}")


@pytest.mark.ssh
@pytest.mark.container
async def test_rest_multisubmit_parallel(
    db,
    tmp777_path,
    fractal_ssh,
    fake_rest,
    history_mock_for_multisubmit,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    valid_user_id,
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]
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
    assert len(fake_rest.submitted) >= 1

    fractal_ssh.run_command(cmd=f"chmod -R 777 {tmp777_path / 'user'}")


@pytest.mark.ssh
@pytest.mark.container
def test_rest_scancel_and_poll_failures(
    tmp777_path,
    fractal_ssh,
    fake_rest,
    monkeypatch,
    slurm_ssh_resource_profile_db: tuple[Resource, Profile],
    fractal_job_id_mock,
):
    res, prof = slurm_ssh_resource_profile_db[:]
    monkeypatch.setattr(runner_module, "MAX_POLL_FAILURES", 2)

    with _get_runner(
        tmp777_path, fractal_ssh, res, prof, fractal_job_id_mock
    ) as runner:
        runner.jobs = {"11": None, "12": None}
        assert runner.scancel_jobs() == ["11", "12"]
        assert fake_rest.cancelled == ["11", "12"]

        def _fail():
            raise RuntimeError("network down")

        fake_rest.get_job_states = _fail
        assert runner.run_squeue(job_ids=["11"]) == (
            "11 FRACTAL_STATUS_PLACEHOLDER"
        )
        with pytest.raises(RuntimeError, match="network down"):
            runner.run_squeue(job_ids=["11"])
