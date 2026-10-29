import io
import json
import subprocess
import types
import typing
import urllib.error

import pytest

from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.exceptions import SlurmConfigError
from fractal_server.runner.executors.slurm_common.slurm_config import (
    SlurmConfig,
)
from fractal_server.runner.executors.slurm_rest import client as client_module
from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.job_description import (
    build_job_description,
)
from fractal_server.runner.executors.slurm_rest.job_description import (
    slurm_time_to_minutes,
)
from fractal_server.runner.executors.slurm_rest.job_description import (
    validate_sbatch_lines,
)
from fractal_server.runner.executors.slurm_rest.job_script import (
    build_job_script,
)
from fractal_server.runner.executors.slurm_rest.runner import MAX_POLL_FAILURES
from fractal_server.runner.executors.slurm_rest.runner import STATUS_PLACEHOLDER
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.executors.slurm_rest.settings import (
    SlurmRestSettings,
)
from fractal_server.runner.executors.slurm_rest.settings import (
    get_slurm_rest_settings,
)
from fractal_server.runner.executors.slurm_rest.settings import (
    is_slurm_rest_enabled,
)
from fractal_server.runner.executors.slurm_rest.token import SlurmTokenError
from fractal_server.runner.v2 import _slurm_rest as slurm_rest_backend
from fractal_server.syringe import Inject
from tests.v2._aux_runner import get_default_slurm_config

FRACTAL_SCRIPT = """#!/bin/sh
#SBATCH --partition=main
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=3
#SBATCH --mem=600M
#SBATCH --job-name=my_task
#SBATCH --time=1-02:03:04
#SBATCH --account=acc
#SBATCH --err=/remote/wf/0_par-slurm-%j.err
#SBATCH --out=/remote/wf/0_par-slurm-%j.out
#SBATCH -D /remote/wf
export FOO=/cache/foo

echo "Hostname: $(uname -n)"
"""


@pytest.mark.parametrize(
    "value,minutes",
    [
        ("10", 10),
        ("10:30", 11),
        ("02:00:00", 120),
        ("1-0", 1440),
        ("1-02:03", 1563),
        ("1-02:03:04", 1564),
    ],
)
def test_slurm_time_to_minutes(value, minutes):
    assert slurm_time_to_minutes(value) == minutes


def test_slurm_time_to_minutes_invalid():
    with pytest.raises(SlurmConfigError):
        slurm_time_to_minutes("1:2:3:4")
    with pytest.raises(SlurmConfigError):
        slurm_time_to_minutes("UNLIMITED")


def test_build_job_description():
    job = build_job_description(
        script=FRACTAL_SCRIPT,
        environment=["PATH=/usr/bin:/bin"],
    )
    assert job == {
        "partition": "main",
        "tasks": 2,
        "cpus_per_task": 3,
        "memory_per_node": {"set": True, "number": 600},
        "name": "my_task",
        "time_limit": {"set": True, "number": 1564},
        "account": "acc",
        "standard_error": "/remote/wf/0_par-slurm-%j.err",
        "standard_output": "/remote/wf/0_par-slurm-%j.out",
        "current_working_directory": "/remote/wf",
        "environment": ["PATH=/usr/bin:/bin", "SLURM_CPUS_PER_TASK=3"],
    }


def test_build_job_description_variants():
    job = build_job_description(
        script=(
            "#SBATCH --mem-per-cpu=2G\n"
            "#SBATCH -p short\n"
            "#SBATCH --qos high\n"
            "#SBATCH --gres=gpu:v100:1\n"
        ),
        environment=[],
    )
    assert job == {
        "memory_per_cpu": {"set": True, "number": 2000},
        "partition": "short",
        "qos": "high",
        "tres_per_node": "gres/gpu:v100:1",
        "environment": [],
    }
    assert "memory_per_node" not in job

    with pytest.raises(SlurmConfigError, match="not supported"):
        build_job_description(
            script="#SBATCH --mail-type=ALL\n", environment=[]
        )
    with pytest.raises(SlurmConfigError, match="Unsupported"):
        build_job_description(script="#SBATCH -Z 1\n", environment=[])


@pytest.mark.parametrize("use_mem_per_cpu", [False, True])
def test_build_job_description_covers_slurm_config(use_mem_per_cpu):
    # Fails when `SlurmConfig` gains an option the REST runner cannot map
    config = get_default_slurm_config()
    for name, field in SlurmConfig.model_fields.items():
        if getattr(config, name) is None:
            is_int = int in typing.get_args(field.annotation)
            setattr(config, name, 1 if is_int else "1")
    lines = config.to_sbatch_preamble(
        remote_export_dir="/remote", use_mem_per_cpu=use_mem_per_cpu
    )
    sbatch_lines = [line for line in lines if line.startswith("#SBATCH")]
    job = build_job_description(script="\n".join(lines), environment=[])
    # One JSON field per option, plus `environment`
    assert len(job) == len(sbatch_lines) + 1


def test_validate_sbatch_lines():
    validate_sbatch_lines(["#SBATCH --qos=high", "export X=1", "module load y"])
    with pytest.raises(SlurmConfigError, match="not supported"):
        validate_sbatch_lines(["#SBATCH --mail-type=ALL"])


def test_build_job_script_writes_files(tmp_path):
    input_path = tmp_path / "x-input.json"
    args_path = tmp_path / "x-args.json"
    copy_path = tmp_path / "x-slurm-submit.sh"
    tricky = json.dumps({"a": "$HOME `id` $(id) 'q' \"dq\" \\"}, indent=2)
    script = build_job_script(
        script=FRACTAL_SCRIPT.replace("/cache/foo", str(tmp_path)),
        files=[(str(input_path), '{"k": 1}'), (str(args_path), tricky)],
        script_copy_path=str(copy_path),
    )
    lines = script.splitlines()
    assert lines[0] == "#!/bin/sh"
    assert all(line.startswith("#SBATCH") for line in lines[1:11])
    assert lines[11] == "umask 0002"
    assert lines[12].startswith(f"cat > '{input_path}'")

    script_path = tmp_path / "submit.sh"
    script_path.write_text(script)
    subprocess.run(["sh", str(script_path)], check=True, cwd=tmp_path)
    assert json.loads(input_path.read_text()) == {"k": 1}
    assert args_path.read_text() == f"{tricky}\n"
    assert copy_path.read_text() == script


def test_build_job_script_limits():
    with pytest.raises(ValueError, match="Single quotes"):
        build_job_script(script="#!/bin/sh\n", files=[("/a'b", "")])
    with pytest.raises(ValueError, match="limit"):
        build_job_script(
            script="#!/bin/sh\n", files=[("/a", "x" * 5 * 1024 * 1024)]
        )


def test_settings(monkeypatch, tmp_path):
    # Avoid reading a local `.fractal_server.env`
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FRACTAL_SLURM_REST_URL", raising=False)
    monkeypatch.setenv("FRACTAL_SLURM_REST_ENABLED", "true")
    with pytest.raises(ValueError, match="FRACTAL_SLURM_REST_URL"):
        SlurmRestSettings()
    monkeypatch.setenv("FRACTAL_SLURM_REST_URL", "https://example.org/")
    settings = SlurmRestSettings()
    assert settings.base_url == "https://example.org"
    for url in ("http://example.org", "example.org"):
        with pytest.raises(ValueError, match="FRACTAL_SLURM_REST_URL"):
            SlurmRestSettings(FRACTAL_SLURM_REST_URL=url)
    with pytest.raises(ValueError, match="not found"):
        SlurmRestSettings(FRACTAL_SLURM_REST_CA_BUNDLE=str(tmp_path / "x"))
    ca_bundle = tmp_path / "ca.pem"
    ca_bundle.write_text("")
    settings = SlurmRestSettings(FRACTAL_SLURM_REST_CA_BUNDLE=str(ca_bundle))
    assert settings.FRACTAL_SLURM_REST_CA_BUNDLE == str(ca_bundle)

    Inject.override(get_slurm_rest_settings, lambda: settings)
    try:
        assert is_slurm_rest_enabled()
    finally:
        Inject.pop(get_slurm_rest_settings)


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _http_error(code: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "url", code, "msg", {}, io.BytesIO(json.dumps(body).encode())
    )


def test_client(monkeypatch):
    calls = []
    responses = []

    def fake_urlopen(request, timeout, context):
        calls.append(
            (
                request.get_method(),
                request.full_url,
                request.get_header("X-slurm-user-token"),
                request.data,
            )
        )
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _FakeResponse(json.dumps(response).encode())

    monkeypatch.setattr(client_module.urllib.request, "urlopen", fake_urlopen)

    tokens = iter(["tok1", "tok2", "tok3"])
    current = {}

    def get_token(force_refresh: bool = False) -> str:
        if force_refresh or "token" not in current:
            current["token"] = next(tokens)
        return current["token"]

    client = SlurmRestClient(
        base_url="https://example.org/",
        username="user",
        get_token=get_token,
    )

    # Submit, with a 401 + token refresh
    responses.extend([_http_error(401, {}), {"job_id": 123}])
    assert client.submit_job({"script": "s", "job": {}}) == 123
    assert calls[0][:3] == (
        "POST",
        "https://example.org/slurm/v0.0.44/job/submit",
        "tok1",
    )
    assert calls[1][2] == "tok2"
    assert json.loads(calls[1][3]) == {"script": "s", "job": {}}

    # Expired token: slurmrestd answers 511, also triggering a refresh
    responses.extend([_http_error(511, {}), {"job_id": 124}])
    assert client.submit_job({"script": "s", "job": {}}) == 124
    assert calls[2][2] == "tok2"
    assert calls[3][2] == "tok3"

    # Job states
    responses.append(
        {
            "jobs": [
                {"job_id": 1, "job_state": ["COMPLETED", "COMPLETING"]},
                {"job_id": 2, "job_state": ["RUNNING"]},
            ]
        }
    )
    assert client.get_job_states() == {"1": "COMPLETED", "2": "RUNNING"}

    # Errors
    responses.append(
        _http_error(500, {"errors": [{"description": "Batch job failed"}]})
    )
    with pytest.raises(SlurmRestError, match="HTTP 500.*Batch job failed"):
        client.submit_job({"script": "s", "job": {}})
    responses.append({"errors": [{"error": "bad"}]})
    with pytest.raises(SlurmRestError, match="bad"):
        client.cancel_job("1")
    assert calls[-1][:2] == (
        "DELETE",
        "https://example.org/slurm/v0.0.44/job/1",
    )

    # Unknown JSON fields are only a warning: cancel the job and fail
    warning = (
        'Ignoring unknown field "x" of type string in DATA_PARSER_JOB_DESC_MSG'
    )
    responses.extend(
        [{"job_id": 125, "warnings": [{"description": warning}]}, {}]
    )
    with pytest.raises(SlurmRestError, match="unknown fields"):
        client.submit_job({"script": "s", "job": {"x": "y"}})
    assert calls[-1][:2] == (
        "DELETE",
        "https://example.org/slurm/v0.0.44/job/125",
    )


def test_client_url_scheme():
    with pytest.raises(ValueError, match="URL scheme"):
        SlurmRestClient(
            base_url="file:///etc",
            username="user",
            get_token=lambda force_refresh=False: "token",
        )


class _FakeStatesClient:
    """
    `get_job_states` raises the queued exceptions, then returns `states`.
    """

    def __init__(self, errors: list[Exception], states: dict[str, str]):
        self.errors = errors
        self.states = states

    def get_job_states(self) -> dict[str, str]:
        if self.errors:
            raise self.errors.pop(0)
        return self.states


def _runner_with_client(client: _FakeStatesClient) -> SlurmRestRunner:
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.rest_client = client
    return runner


def test_run_squeue_waits_for_token():
    """
    Without a valid token, jobs stay active for longer than
    `MAX_POLL_FAILURES` polls, and are not marked as completed.
    """
    n_errors = MAX_POLL_FAILURES + 5
    runner = _runner_with_client(
        _FakeStatesClient(
            errors=[SlurmTokenError("log in again")] * n_errors,
            states={"1": "RUNNING", "2": "COMPLETED"},
        )
    )
    for _ in range(n_errors):
        assert runner._get_finished_jobs(job_ids=["1", "2"]) == set()
        assert runner._poll_failures == 0
        assert runner._waiting_for_token
    assert runner._get_finished_jobs(job_ids=["1", "2"]) == {"2"}
    assert not runner._waiting_for_token


def test_run_squeue_poll_failures():
    runner = _runner_with_client(
        _FakeStatesClient(
            errors=[SlurmRestError("down")] * MAX_POLL_FAILURES,
            states={},
        )
    )
    for _ in range(MAX_POLL_FAILURES - 1):
        assert runner.run_squeue(job_ids=["1"]) == f"1 {STATUS_PLACEHOLDER}"
    with pytest.raises(SlurmRestError, match="down"):
        runner.run_squeue(job_ids=["1"])
    assert runner.run_squeue(job_ids=[]) == ""


def test_process_workflow_token_error(monkeypatch, tmp_path):
    def _fail(**kwargs):
        raise SlurmTokenError("Hint: the user must log in to Fractal again.")

    monkeypatch.setattr(slurm_rest_backend, "SlurmRestRunner", _fail)
    with pytest.raises(JobExecutionError) as exc_info:
        slurm_rest_backend.process_workflow(
            job_id=1,
            workflow=types.SimpleNamespace(task_list=[None]),
            dataset=None,
            workflow_dir_local=tmp_path,
            logger_name="test",
            job_attribute_filters={},
            job_type_filters={},
            user_id=1,
            resource=types.SimpleNamespace(id=1),
            profile=None,
            user_cache_dir=str(tmp_path),
        )
    assert "log in to Fractal again" in exc_info.value.assemble_error()
