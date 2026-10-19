"""
Opt-in tests against a real `slurmrestd`, skipped unless configured.

Required environment variables:
    FRACTAL_TEST_SLURM_REST_URL: base URL of `slurmrestd`.
    FRACTAL_TEST_SLURM_REST_USER: SLURM user name.
    FRACTAL_TEST_SLURM_REST_TOKEN: valid JWT, e.g. from `scontrol token`.
    FRACTAL_TEST_SLURM_REST_WORKDIR: remote folder writable by the user.

Optional environment variables:
    FRACTAL_TEST_SLURM_REST_API_VERSION: default `v0.0.44`.
    FRACTAL_TEST_SLURM_REST_CA_BUNDLE: CA file (PEM) for TLS verification.
    FRACTAL_TEST_SLURM_REST_PARTITION: SLURM partition.
"""

import os
import time
from uuid import uuid4

import pytest

from fractal_server.runner.executors.slurm_common._job_states import (
    STATES_FINISHED,
)
from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.job_description import (
    build_job_description,
)
from fractal_server.runner.executors.slurm_rest.script import build_rest_script

ENV_PREFIX = "FRACTAL_TEST_SLURM_REST_"
REQUIRED = ["URL", "USER", "TOKEN", "WORKDIR"]
POLL_TIMEOUT = 300

pytestmark = pytest.mark.skipif(
    any(not os.environ.get(f"{ENV_PREFIX}{key}") for key in REQUIRED),
    reason=f"Set {', '.join(ENV_PREFIX + key for key in REQUIRED)}.",
)


def _env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(f"{ENV_PREFIX}{key}", default)


def _client(token: str | None = None) -> SlurmRestClient:
    token = token or _env("TOKEN")
    return SlurmRestClient(
        base_url=_env("URL"),
        api_version=_env("API_VERSION", "v0.0.44"),
        username=_env("USER"),
        get_token=lambda force_refresh=False: token,
        ca_bundle=_env("CA_BUNDLE"),
    )


def _request_body(*, body: str, files: list[tuple[str, str]]) -> dict:
    workdir = _env("WORKDIR").rstrip("/")
    name = f"fractal-rest-test-{uuid4().hex[:8]}"
    header = [
        "#!/bin/bash",
        f"#SBATCH --job-name={name}",
        "#SBATCH --ntasks=1",
        "#SBATCH --cpus-per-task=1",
        "#SBATCH --mem=100M",
        "#SBATCH --time=10",
        f"#SBATCH --output={workdir}/{name}-%j.out",
        f"#SBATCH --error={workdir}/{name}-%j.err",
    ]
    if _env("PARTITION"):
        header.append(f"#SBATCH --partition={_env('PARTITION')}")
    script = "\n".join(header + [body]) + "\n"
    return dict(
        script=build_rest_script(script=script, files=files),
        job=build_job_description(
            script=script,
            environment=[
                "PATH=/usr/local/bin:/usr/bin:/bin",
                f"HOME={workdir}",
                f"USER={_env('USER')}",
            ],
        ),
    )


def _wait_finished(client: SlurmRestClient, job_id: str) -> str:
    deadline = time.monotonic() + POLL_TIMEOUT
    while time.monotonic() < deadline:
        state = client.get_job_states().get(job_id)
        if state in STATES_FINISHED:
            return state
        time.sleep(3)
    raise TimeoutError(f"SLURM job {job_id} not finished after {POLL_TIMEOUT}s")


def test_live_ping():
    _client().ping()


def test_live_submit_completed():
    client = _client()
    input_path = f"{_env('WORKDIR').rstrip('/')}/input-{uuid4().hex}.txt"
    body = _request_body(
        body=(
            f"grep -q 'fractal-rest-test' '{input_path}'\n"
            'test "$SLURM_CPUS_PER_TASK" = 1\n'
            f"rm '{input_path}'"
        ),
        files=[(input_path, "fractal-rest-test\n")],
    )
    job_id = str(client.submit_job(body))
    assert _wait_finished(client, job_id) == "COMPLETED"


def test_live_cancel():
    client = _client()
    job_id = str(client.submit_job(_request_body(body="sleep 300", files=[])))
    client.cancel_job(job_id)
    assert _wait_finished(client, job_id) == "CANCELLED"


def test_live_invalid_token():
    # `/ping` does not check authentication, `/jobs` does
    with pytest.raises(SlurmRestError):
        _client(token="invalid-token").get_job_states()
