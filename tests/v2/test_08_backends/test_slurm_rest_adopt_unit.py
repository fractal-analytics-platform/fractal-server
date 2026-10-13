import json
from pathlib import Path

import pytest

from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (
    SlurmJob,
)
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.executors.slurm_rest.runner import normalize_script
from fractal_server.runner.executors.slurm_rest.script import build_rest_script
from fractal_server.runner.v2.resume.slurm_evidence import FractalSlurmJob

SCRIPT = "#!/bin/sh\n#SBATCH -D /remote/1_t\nsrun remote.py\n"
FILES = [("/remote/1_t/x-args.json", '{"a": 1}')]


class _FakeClient:
    def __init__(self, cancel_error: bool = False, states=None):
        self.cancelled = []
        self.cancel_error = cancel_error
        self.states = states or {}

    def cancel_job(self, job_id):
        if self.cancel_error:
            raise SlurmRestError("DELETE failed")
        self.cancelled.append(job_id)

    def get_job_states(self):
        return self.states


class _FakeSSH:
    def __init__(self, files: dict[str, str]):
        self.files = files

    def remote_exists(self, path):
        return path in self.files

    def read_remote_text_file(self, path):
        return self.files[path]


def _runner(remote_files: dict[str, str] | None = None) -> SlurmRestRunner:
    # Skip the constructor (it needs SSH and slurmrestd)
    runner = object.__new__(SlurmRestRunner)
    runner.adopt = True
    runner.jobs = {}
    runner._adopted_ids = set()
    runner.rest_client = _FakeClient()
    runner.fractal_ssh = _FakeSSH(remote_files or {})
    return runner


def _slurm_job(tmp_path: Path) -> SlurmJob:
    return SlurmJob(
        prefix="par-000000",
        workdir_local=tmp_path / "1_t",
        workdir_remote=Path("/remote/1_t"),
        tasks=[],
    )


def _found(state: str) -> FractalSlurmJob:
    return FractalSlurmJob(
        slurm_job_id="42",
        state=state,
        subfolder="1_t",
        prefix="par-000000",
        source="slurm",
    )


def test_normalize_script():
    script_1 = build_rest_script(script=SCRIPT, files=FILES)
    script_2 = build_rest_script(script=SCRIPT, files=FILES)
    assert script_1 != script_2
    assert normalize_script(script_1) == normalize_script(script_2)


def test_adopt_same_script(tmp_path):
    runner = _runner()
    slurm_job = _slurm_job(tmp_path)
    assert runner._adopt(
        slurm_job=slurm_job,
        found=_found("RUNNING"),
        previous_script=build_rest_script(script=SCRIPT, files=FILES),
        script=build_rest_script(script=SCRIPT, files=FILES),
    )
    assert runner.jobs == {"42": slurm_job}
    assert slurm_job.slurm_job_id == "42"
    # Adopted jobs are not accounted for again
    assert runner.job_ids_int == []


def test_adopt_different_script(tmp_path):
    runner = _runner()
    with pytest.raises(JobExecutionError, match="different script"):
        runner._adopt(
            slurm_job=_slurm_job(tmp_path),
            found=_found("COMPLETED"),
            previous_script=build_rest_script(script=SCRIPT, files=FILES),
            script=build_rest_script(
                script=SCRIPT, files=[(FILES[0][0], '{"a": 2}')]
            ),
        )
    assert runner.jobs == {}


@pytest.mark.parametrize(
    "state,cancelled",
    [("PENDING", ["42"]), ("STAGE_OUT", ["42"]), ("FAILED", [])],
)
def test_adopt_unverifiable(tmp_path, state, cancelled):
    runner = _runner()
    assert not runner._adopt(
        slurm_job=_slurm_job(tmp_path),
        found=_found(state),
        previous_script=None,
        script=SCRIPT,
    )
    assert runner.rest_client.cancelled == cancelled
    assert runner.jobs == {}


@pytest.mark.parametrize(
    "states,raises",
    [({"42": "COMPLETED"}, False), ({}, False), ({"42": "RUNNING"}, True)],
)
def test_adopt_unverifiable_cancel_fails(tmp_path, states, raises):
    runner = _runner()
    runner.rest_client = _FakeClient(cancel_error=True, states=states)
    kwargs = dict(
        slurm_job=_slurm_job(tmp_path),
        found=_found("RUNNING"),
        previous_script=None,
        script=SCRIPT,
    )
    if raises:
        with pytest.raises(SlurmRestError):
            runner._adopt(**kwargs)
    else:
        assert not runner._adopt(**kwargs)


def test_previous_script(tmp_path):
    slurm_job = _slurm_job(tmp_path)
    request_path = tmp_path / "request.json"
    remote_copy = slurm_job.slurm_submission_script_remote

    runner = _runner()
    assert (
        runner._previous_script(slurm_job=slurm_job, request_path=request_path)
        is None
    )

    runner = _runner({remote_copy: "remote"})
    assert (
        runner._previous_script(slurm_job=slurm_job, request_path=request_path)
        == "remote"
    )

    # The local request file is preferred
    request_path.write_text(json.dumps({"script": "local"}))
    assert (
        runner._previous_script(slurm_job=slurm_job, request_path=request_path)
        == "local"
    )
