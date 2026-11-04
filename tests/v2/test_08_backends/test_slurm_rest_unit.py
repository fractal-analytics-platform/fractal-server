import io
import json
import subprocess
import types
import typing
import urllib.error
from unittest.mock import MagicMock

import pytest

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.exceptions import SlurmConfigError
from fractal_server.runner.executors.slurm_common import (
    base_slurm_runner as base_runner_module,
)
from fractal_server.runner.executors.slurm_common.slurm_config import (
    SlurmConfig,
)
from fractal_server.runner.executors.slurm_rest import client as client_module
from fractal_server.runner.executors.slurm_rest import runner as runner_module
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
from fractal_server.runner.executors.slurm_rest.runner import STATUS_PLACEHOLDER
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.executors.slurm_rest.settings import (
    SlurmRestSettings,
)
from fractal_server.runner.executors.slurm_rest.settings import (
    get_slurm_rest_settings,
)
from fractal_server.runner.executors.slurm_rest.token import SlurmTokenError
from fractal_server.runner.v2 import _local as local_backend
from fractal_server.runner.v2 import _slurm_rest as slurm_rest_backend
from fractal_server.runner.v2 import _slurm_ssh as slurm_ssh_backend
from fractal_server.runner.v2 import _slurm_sudo as slurm_sudo_backend
from fractal_server.runner.v2.submit_workflow import _get_process_workflow
from fractal_server.ssh._fabric import FractalSSHCommandError
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

    with pytest.raises(SlurmConfigError, match="not supported"):
        build_job_description(
            script="#SBATCH --mail-type=ALL\n", environment=[]
        )
    with pytest.raises(SlurmConfigError, match="Unsupported"):
        build_job_description(script="#SBATCH -Z 1\n", environment=[])


@pytest.mark.parametrize(
    "ntasks,nodes,expected",
    [(1, 2, 1), (2, 2, 2), (4, 2, 2), (None, 3, 3)],
)
def test_build_job_description_nodes(ntasks, nodes, expected):
    """
    As `sbatch`, at most one node per task.
    """
    script = f"#SBATCH --nodes={nodes}\n"
    if ntasks is not None:
        script += f"#SBATCH --ntasks={ntasks}\n"
    job = build_job_description(script=script, environment=[])
    assert job["minimum_nodes"] == job["maximum_nodes"] == expected


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
    # One JSON field per option (two for `--nodes`), plus `environment`
    assert len(job) == len(sbatch_lines) + 2


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
    # Values are read from the environment
    monkeypatch.setenv("FRACTAL_SLURM_REST_URL", "https://example.org/")
    settings = SlurmRestSettings(_env_file=None)
    assert settings.base_url == "https://example.org"

    def _settings(**kwargs) -> SlurmRestSettings:
        return SlurmRestSettings(
            _env_file=None,
            **(
                dict(
                    FRACTAL_SLURM_REST_ENABLED="true",
                    FRACTAL_SLURM_REST_URL="https://example.org",
                    FRACTAL_SLURM_REST_CA_BUNDLE=None,
                )
                | kwargs
            ),
        )

    with pytest.raises(ValueError, match="FRACTAL_SLURM_REST_URL"):
        _settings(FRACTAL_SLURM_REST_URL=None)
    for url in ("http://example.org", "example.org"):
        with pytest.raises(ValueError, match="FRACTAL_SLURM_REST_URL"):
            _settings(FRACTAL_SLURM_REST_URL=url)
    with pytest.raises(ValueError, match="not found"):
        _settings(FRACTAL_SLURM_REST_CA_BUNDLE=str(tmp_path / "x"))
    ca_bundle = tmp_path / "ca.pem"
    ca_bundle.write_text("")
    settings = _settings(FRACTAL_SLURM_REST_CA_BUNDLE=str(ca_bundle))
    assert settings.FRACTAL_SLURM_REST_CA_BUNDLE == str(ca_bundle)


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
    # Both the generic description and the specific error (JUNO answer)
    responses.append(
        _http_error(
            500,
            {
                "errors": [
                    {
                        "description": "Batch job submission failed",
                        "error_number": 2063,
                        "error": "Job cannot be submitted without the "
                        "current working directory specified.",
                    }
                ]
            },
        )
    )
    with pytest.raises(
        SlurmRestError, match="submission failed: Job cannot be submitted"
    ):
        client.submit_job({"script": "s", "job": {}})
    # (Errors with neither field fall back to the whole error: not tested)
    # Not JSON
    responses.append(
        urllib.error.HTTPError(
            "url", 502, "msg", {}, io.BytesIO(b"<html>Bad Gateway</html>")
        )
    )
    with pytest.raises(SlurmRestError, match="HTTP 502.*Bad Gateway"):
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


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Service unavailable</html>",
        b"[]",
        b"{}",
        b'{"jobs": null}',
        b'{"jobs": {}}',
    ],
)
def test_job_states_invalid_response(monkeypatch, body):
    """
    A successful (HTTP 200) response without a job list, e.g. an HTML page
    from a proxy in front of `slurmrestd`, is an error: it must not mean "no
    jobs", which would mark all jobs as completed.
    """
    monkeypatch.setattr(
        client_module.urllib.request,
        "urlopen",
        lambda request, timeout, context: _FakeResponse(body),
    )
    client = SlurmRestClient(
        base_url="https://example.org",
        username="user",
        get_token=lambda force_refresh=False: "token",
    )
    with pytest.raises(SlurmRestError):
        client.get_job_states()
    runner = _runner_with_client(client)
    assert runner._get_finished_jobs(job_ids=["1"]) == set()

    # An empty job list is valid: jobs are missing, hence completed
    monkeypatch.setattr(
        client_module.urllib.request,
        "urlopen",
        lambda request, timeout, context: _FakeResponse(b'{"jobs": []}'),
    )
    assert client.get_job_states() == {}
    assert runner._get_finished_jobs(job_ids=["1"]) == {"1"}


@pytest.mark.parametrize("base_url", ["file:///etc", "http://example.org"])
def test_client_url_scheme(base_url):
    with pytest.raises(ValueError, match="URL scheme"):
        SlurmRestClient(
            base_url=base_url,
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


@pytest.mark.parametrize(
    "error",
    [
        SlurmTokenError("log in again"),
        SlurmRestError("GET /jobs -> HTTP 500"),
        OSError("Connection refused"),
    ],
)
def test_run_squeue_failed_queries_keep_jobs_active(error):
    """
    Failed status queries (no valid token, or `slurmrestd` unreachable or
    failing) never mark jobs as completed, with no limit on their number,
    and the job states are used again as soon as a query succeeds.
    """
    n_errors = 100
    runner = _runner_with_client(
        _FakeStatesClient(
            errors=[error] * n_errors,
            states={"1": "RUNNING", "2": "COMPLETED"},
        )
    )
    for _ in range(n_errors):
        assert runner._get_finished_jobs(job_ids=["1", "2"]) == set()
    assert runner._get_finished_jobs(job_ids=["1", "2"]) == {"2"}
    assert runner.run_squeue(job_ids=[]) == ""


def test_run_squeue_missing_jobs_are_finished():
    """
    Jobs not listed by `GET /jobs` (e.g. finished for longer than `MinJobAge`)
    are omitted, and then treated as finished, as with `squeue`.
    """
    runner = _runner_with_client(
        _FakeStatesClient(errors=[], states={"1": "RUNNING", "3": "PENDING"})
    )
    assert runner.run_squeue(job_ids=["1", "2"]) == "1 RUNNING"
    assert runner._get_finished_jobs(job_ids=["1", "2"]) == {"2"}


def test_run_squeue_logs_once(monkeypatch):
    """
    While waiting for a login or for `slurmrestd` (possibly for days), a
    warning at every poll (every few seconds) would flood the server logs:
    only the start and the end of each problem are logged.
    """
    logger = MagicMock()
    monkeypatch.setattr(runner_module, "logger", logger)
    runner = _runner_with_client(
        _FakeStatesClient(
            errors=[SlurmRestError("down")] * 3
            + [SlurmTokenError("log in again")] * 3,
            states={"1": "RUNNING"},
        )
    )
    for _ in range(6):
        assert runner.run_squeue(job_ids=["1"]) == f"1 {STATUS_PLACEHOLDER}"
    assert runner.run_squeue(job_ids=["1"]) == "1 RUNNING"
    messages = [
        call.args[0]
        for call in logger.warning.call_args_list + logger.info.call_args_list
    ]
    assert sum("Cannot query SLURM job states" in m for m in messages) == 1
    assert sum("No valid SLURM token" in m for m in messages) == 1
    assert sum("available again" in m for m in messages) == 1


class _FakeCancelClient:
    def __init__(self, failing: set[str]):
        self.failing = failing
        self.cancelled: list[str] = []

    def cancel_job(self, job_id: str) -> None:
        self.cancelled.append(job_id)
        if job_id in self.failing:
            raise SlurmRestError(f"DELETE /job/{job_id} -> HTTP 500")


def test_scancel_jobs():
    """
    One cancel request per SLURM job; a failure does not stop the others.
    """
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.rest_client = _FakeCancelClient(failing={"12"})
    runner.jobs = {"11": None, "12": None, "13": None}
    assert runner.scancel_jobs() == ["11", "12", "13"]
    assert runner.rest_client.cancelled == ["11", "12", "13"]

    runner.jobs = {}
    assert runner.scancel_jobs() == []


class _FakeDownClient:
    """
    No valid token, or `slurmrestd` down: every request fails.
    """

    def __init__(self):
        self.requests: list[str] = []

    def get_job_states(self) -> dict[str, str]:
        self.requests.append("GET /jobs")
        raise SlurmTokenError("log in again")

    def cancel_job(self, job_id: str) -> None:
        self.requests.append(f"DELETE /job/{job_id}")
        raise SlurmTokenError("log in again")


def test_stop_while_requests_fail(tmp_path, monkeypatch):
    """
    Stopping a job while SLURM jobs can be neither polled nor cancelled: the
    jobs stay tracked, and polling and cancellation are retried once per
    `poll_interval`, not in a busy loop. Only the start and the end of the
    problem are logged above debug level, not every retry.
    """
    sleeps = []
    monkeypatch.setattr(runner_module.time, "sleep", sleeps.append)
    rest_logger = MagicMock()
    base_logger = MagicMock()
    monkeypatch.setattr(runner_module, "logger", rest_logger)
    monkeypatch.setattr(base_runner_module, "logger", base_logger)
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.rest_client = _FakeDownClient()
    runner.jobs = {"1": None}
    runner.poll_interval = 5
    runner.poll_interval_internal = 0.5
    runner.shutdown_file = tmp_path / "shutdown"
    runner.shutdown_file.touch()

    # Same steps as the retrieval loop of `submit` and `multisubmit`
    for _ in range(3):
        assert runner._get_finished_jobs(job_ids=runner.job_ids) == set()
        assert runner.wait_and_check_shutdown() == ["1"]
    assert runner.job_ids == ["1"]
    assert runner.rest_client.requests == ["GET /jobs", "DELETE /job/1"] * 3
    assert sleeps == [5, 5, 5]
    assert rest_logger.error.call_count == 1
    assert rest_logger.warning.call_count == 1  # From `run_squeue`
    assert rest_logger.info.call_count == 3  # START, count, END
    base_messages = [call.args[0] for call in base_logger.info.call_args_list]
    assert sum("Shutdown file detected" in m for m in base_messages) == 1

    # Cancellation is possible again
    runner.rest_client = _FakeCancelClient(failing=set())
    assert runner.wait_and_check_shutdown() == ["1"]
    assert runner.rest_client.cancelled == ["1"]
    assert sleeps == [5, 5, 5, 5]
    messages = [call.args[0] for call in rest_logger.info.call_args_list]
    assert sum("Cancel possible again" in m for m in messages) == 1
    assert not runner._cancel_failed


class _FakeSSH:
    def __init__(self, output: str | Exception):
        self.output = output
        self.commands = []

    def run_command(self, *, cmd: str) -> str:
        self.commands.append(cmd)
        if isinstance(self.output, Exception):
            raise self.output
        return self.output


def test_get_slurm_home():
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.slurm_username = "alice"
    runner.fractal_ssh = _FakeSSH("alice:x:1:2::/home/alice:/bin/bash\n")
    assert runner._get_slurm_home() == "/home/alice"
    assert runner.fractal_ssh.commands == ["getent passwd alice"]

    runner.slurm_username = "a b;c"
    runner._get_slurm_home()
    assert runner.fractal_ssh.commands[-1] == "getent passwd 'a b;c'"

    for output in (FractalSSHCommandError("exit code 2"), ""):
        runner.fractal_ssh = _FakeSSH(output)
        with pytest.raises(JobExecutionError) as exc_info:
            runner._get_slurm_home()
        assert "unknown on the cluster" in exc_info.value.assemble_error()


class _FakePingClient:
    base_url = "https://example.org/slurm/v0.0.44"

    def __init__(self, error: Exception | None):
        self.error = error

    def ping(self) -> None:
        if self.error is not None:
            raise self.error


# Connection, DNS, TLS and timeout errors are all `OSError` (as `URLError`),
# and take the same path: one case is enough
@pytest.mark.parametrize(
    "error",
    [
        SlurmRestError("GET /ping -> HTTP 502: []"),
        urllib.error.URLError(ConnectionRefusedError("refused")),
    ],
)
def test_check_slurmrestd(error):
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.rest_client = _FakePingClient(None)
    runner._check_slurmrestd()

    runner.rest_client = _FakePingClient(error)
    with pytest.raises(JobExecutionError) as exc_info:
        runner._check_slurmrestd()
    message = exc_info.value.assemble_error()
    assert "Cannot reach slurmrestd at https://example.org" in message
    assert type(error).__name__ in message

    # Token errors are left to `process_workflow`
    runner.rest_client = _FakePingClient(SlurmTokenError("log in again"))
    with pytest.raises(SlurmTokenError):
        runner._check_slurmrestd()


@pytest.mark.parametrize("rest_enabled", ["true", "false"])
def test_get_process_workflow(rest_enabled):
    """
    `slurm_ssh` resources use the REST backend if and only if
    `FRACTAL_SLURM_REST_ENABLED=true`; other resource types never do.
    """
    settings = SlurmRestSettings(
        _env_file=None,
        FRACTAL_SLURM_REST_ENABLED=rest_enabled,
        FRACTAL_SLURM_REST_URL="https://slurmrestd.example.org",
        FRACTAL_SLURM_REST_CA_BUNDLE=None,
    )
    Inject.override(get_slurm_rest_settings, lambda: settings)
    try:
        assert _get_process_workflow(ResourceType.SLURM_SSH) is (
            slurm_rest_backend.process_workflow
            if rest_enabled == "true"
            else slurm_ssh_backend.process_workflow
        )
        assert _get_process_workflow(ResourceType.SLURM_SUDO) is (
            slurm_sudo_backend.process_workflow
        )
        assert _get_process_workflow(ResourceType.LOCAL) is (
            local_backend.process_workflow
        )
    finally:
        Inject.pop(get_slurm_rest_settings)
    with pytest.raises(ValueError, match="Unknown resource type"):
        _get_process_workflow("unknown")


def test_process_workflow(monkeypatch, tmp_path):
    """
    `process_workflow` runs the selected tasks with a `SlurmRestRunner`.
    """
    runners = []
    executed = []

    class _FakeRunner:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            runners.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(slurm_rest_backend, "SlurmRestRunner", _FakeRunner)
    monkeypatch.setattr(
        slurm_rest_backend, "execute_tasks", lambda **kw: executed.append(kw)
    )
    wftasks = ["wftask0", "wftask1", "wftask2"]
    slurm_rest_backend.process_workflow(
        job_id=7,
        workflow=types.SimpleNamespace(task_list=wftasks),
        dataset="dataset",
        workflow_dir_local=tmp_path / "local",
        workflow_dir_remote=tmp_path / "remote",
        first_task_index=1,
        last_task_index=1,
        logger_name="test",
        job_attribute_filters={},
        job_type_filters={},
        user_id=3,
        resource=types.SimpleNamespace(id=5),
        profile="profile",
        fractal_ssh="fractal_ssh",
        slurm_account="account",
        worker_init="module load x",
        user_cache_dir=str(tmp_path),
    )

    # Other arguments are passed on as in `_slurm_ssh.process_workflow`, and
    # are not checked here
    assert len(runners) == 1
    assert runners[0].kwargs["user_id"] == 3
    assert "module load x" in runners[0].kwargs["common_script_lines"]
    assert len(executed) == 1
    assert executed[0]["wf_task_list"] == ["wftask1"]
    assert executed[0]["runner"] is runners[0]


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
