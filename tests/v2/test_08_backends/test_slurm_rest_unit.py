import http.client
import io
import json
import subprocess
import typing
import urllib.error
from unittest.mock import MagicMock

import pytest

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import ValidProfileSlurmRest
from fractal_server.app.schemas.v2 import ValidResourceSlurmRest
from fractal_server.app.schemas.v2.resource import cast_serialize_resource
from fractal_server.runner.config import SlurmRestConfig
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.exceptions import SlurmConfigError
from fractal_server.runner.executors.slurm_common._job_states import (
    STATES_FINISHED,
)
from fractal_server.runner.executors.slurm_common.slurm_config import (
    SlurmConfig,
)
from fractal_server.runner.executors.slurm_rest import client as client_module
from fractal_server.runner.executors.slurm_rest import runner as runner_module
from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.client import squeue_state
from fractal_server.runner.executors.slurm_rest.job_description import OPTIONS
from fractal_server.runner.executors.slurm_rest.job_description import (
    _apply_option,
)
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
from fractal_server.runner.executors.slurm_rest.token import SlurmTokenError
from fractal_server.runner.v2 import _local as local_backend
from fractal_server.runner.v2 import _slurm_rest as slurm_rest_backend
from fractal_server.runner.v2 import _slurm_ssh as slurm_ssh_backend
from fractal_server.runner.v2 import _slurm_sudo as slurm_sudo_backend
from fractal_server.runner.v2.submit_workflow import _get_process_workflow
from fractal_server.ssh._fabric import FractalSSHCommandError
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
    for value in ("1:2:3:4", "1-02:03:04:999", "1-", "UNLIMITED"):
        with pytest.raises(SlurmConfigError, match=value):
            slurm_time_to_minutes(value)


@pytest.mark.parametrize(
    "line,expected",
    [
        # As Fractal's own configuration (`slurm_mem_to_MB`, powers of 1000)
        ("--mem=600", {"memory_per_node": {"set": True, "number": 600}}),
        ("--mem=600M", {"memory_per_node": {"set": True, "number": 600}}),
        ("--mem=2G", {"memory_per_node": {"set": True, "number": 2000}}),
        ("--mem=2GB", {"memory_per_node": {"set": True, "number": 2000}}),
        ("--mem=1T", {"memory_per_node": {"set": True, "number": 1000000}}),
        ("--mem-per-cpu=2G", {"memory_per_cpu": {"set": True, "number": 2000}}),
        # Each GRES gets its `gres/` prefix, as with `sbatch`
        ("--gres=gpu:a100:1", {"tres_per_node": "gres/gpu:a100:1"}),
        (
            "--gres=gpu:a100:1,shard:2",
            {"tres_per_node": "gres/gpu:a100:1,gres/shard:2"},
        ),
    ],
)
def test_build_job_description_values(line, expected):
    job = build_job_description(script=f"#SBATCH {line}\n", environment=[])
    assert job == expected | {"environment": []}


@pytest.mark.parametrize(
    "line,expected",
    [
        ("--partition=main", {"partition": "main"}),
        ("--partition main", {"partition": "main"}),
        ("-p main", {"partition": "main"}),
        ("-pmain", {"partition": "main"}),
        # Unambiguous abbreviations, as with `sbatch`
        ("--err=/x.err", {"standard_error": "/x.err"}),
        # Quotes and comments, as with `sbatch`
        ('--job-name="my job"', {"name": "my job"}),
        ("--job-name='my job'", {"name": "my job"}),
        ("--time=10 # comment", {"time_limit": {"set": True, "number": 10}}),
        # Several options on one line
        ("--qos=high --account=acc", {"qos": "high", "account": "acc"}),
    ],
)
def test_build_job_description_line_syntax(line, expected):
    job = build_job_description(script=f"#SBATCH {line}\n", environment=[])
    assert job == expected | {"environment": []}


def test_options_are_all_handled():
    # Every option accepted by the parser has a JSON mapping
    for name in OPTIONS:
        _apply_option({}, name, "1")


@pytest.mark.parametrize(
    "line",
    [
        "--mem=2X",
        "--mem=2BG",
        "--partition=main extra",
        '--job-name="unclosed',
        "--partition",
        "--mail-type=ALL",
        "-Z 1",
        # Ambiguous abbreviation (`nodelist`, `nodes`, `ntasks`)
        "--n=1",
    ],
)
def test_build_job_description_invalid_values(line):
    with pytest.raises(SlurmConfigError):
        build_job_description(script=f"#SBATCH {line}\n", environment=[])


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


def test_slurm_rest_resource_schema(slurm_ssh_resource_profile_fake_objects):
    resource, profile = slurm_ssh_resource_profile_fake_objects
    ssh_data = resource.model_dump(exclude={"id", "timestamp_created"})
    ssh_runner_config = ssh_data["jobs_runner_config"]

    def _data(**rest) -> dict:
        return ssh_data | dict(
            type=ResourceType.SLURM_REST,
            jobs_runner_config=ssh_runner_config | dict(rest=rest),
        )

    data = _data(url="https://example.org/")
    rest = ValidResourceSlurmRest(**data).jobs_runner_config.rest
    assert rest == SlurmRestConfig(url="https://example.org")
    assert rest.ca_bundle is None
    assert rest.api_version is None
    assert cast_serialize_resource(data)["jobs_runner_config"]["rest"] == dict(
        url="https://example.org", ca_bundle=None, api_version=None
    )
    ValidProfileSlurmRest(
        **(profile.model_dump() | dict(resource_type=ResourceType.SLURM_REST))
    )

    for url in ("http://example.org", "example.org", ""):
        with pytest.raises(ValueError):
            ValidResourceSlurmRest(**_data(url=url))
    with pytest.raises(ValueError, match="rest"):
        ValidResourceSlurmRest(
            **(data | dict(jobs_runner_config=ssh_runner_config))
        )
    with pytest.raises(ValueError):
        ValidResourceSlurmRest(**_data(url="https://a.b", foo="bar"))
    with pytest.raises(ValueError):
        ValidResourceSlurmRest(
            **_data(url="https://a.b", ca_bundle="relative.pem")
        )
    with pytest.raises(ValueError, match="Unsupported"):
        ValidResourceSlurmRest(**_data(url="https://a.b", api_version="v0.0.1"))
    rest = ValidResourceSlurmRest(
        **_data(url="https://a.b", api_version="v0.0.44")
    ).jobs_runner_config.rest
    assert rest.api_version == "v0.0.44"

    # The `rest` key is only valid for `slurm_rest` resources
    with pytest.raises(ValueError):
        cast_serialize_resource(data | dict(type=ResourceType.SLURM_SSH))


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
    assert client.get_job_states() == {"1": "COMPLETING", "2": "RUNNING"}

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
    # A failed cancellation does not hide the unknown fields
    responses.extend(
        [
            {"job_id": 126, "warnings": [{"description": warning}]},
            _http_error(500, {"errors": [{"error": "cancel broken"}]}),
        ]
    )
    with pytest.raises(SlurmRestError) as exc_info:
        client.submit_job({"script": "s", "job": {"x": "y"}})
    assert "unknown fields" in str(exc_info.value)
    assert "job 126 may still be queued" in str(exc_info.value)
    assert "cancel broken" in str(exc_info.value)


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Service unavailable</html>",
        b"{}",
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


@pytest.mark.parametrize(
    "job_state,expected,finished",
    [
        (["RUNNING"], "RUNNING", False),
        (["COMPLETED"], "COMPLETED", True),
        # Epilog still running: not finished yet, as with `squeue`
        (["COMPLETED", "COMPLETING"], "COMPLETING", False),
        (["CANCELLED", "COMPLETING"], "COMPLETING", False),
        (["PENDING", "REQUEUED"], "REQUEUED", False),
        (["PENDING", "SPECIAL_EXIT"], "SPECIAL_EXIT", True),
        # Flags that `squeue` does not report: not finished until cleared
        (["PENDING", "POWER_UP_NODE"], "POWER_UP_NODE", False),
        ([], "UNKNOWN", False),
    ],
)
def test_squeue_state(job_state, expected, finished):
    assert squeue_state(job_state) == expected
    assert (squeue_state(job_state) in STATES_FINISHED) is finished


@pytest.mark.parametrize("base_url", ["file:///etc", "http://example.org"])
def test_client_url_scheme(base_url):
    with pytest.raises(ValueError, match="URL scheme"):
        SlurmRestClient(
            base_url=base_url,
            username="user",
            get_token=lambda force_refresh=False: "token",
        )


def test_client_api_version_and_ca_bundle(tmp_path):
    with pytest.raises(ValueError, match="Unsupported"):
        SlurmRestClient(
            base_url="https://example.org",
            username="user",
            get_token=lambda force_refresh=False: "token",
            api_version="v0.0.1",
        )
    with pytest.raises(SlurmRestError, match="Cannot load CA bundle"):
        SlurmRestClient(
            base_url="https://example.org",
            username="user",
            get_token=lambda force_refresh=False: "token",
            ca_bundle=(tmp_path / "missing.pem").as_posix(),
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
    `poll_interval`, not in a busy loop. The cancel failure is logged as an
    error once, not at every retry.
    """
    sleeps = []
    monkeypatch.setattr(runner_module.time, "sleep", sleeps.append)
    rest_logger = MagicMock()
    monkeypatch.setattr(runner_module, "logger", rest_logger)
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

    # Cancellation is possible again
    runner.rest_client = _FakeCancelClient(failing=set())
    assert runner.wait_and_check_shutdown() == ["1"]
    assert runner.rest_client.cancelled == ["1"]
    assert sleeps == [5, 5, 5, 5]
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


def test_mkdir_remote_folder():
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    runner.fractal_ssh = _FakeSSH("")
    runner._mkdir_remote_folder(folder="/base/job dir")
    assert runner.fractal_ssh.commands == ["mkdir -p -m 2775 '/base/job dir'"]


class _FakePingClient:
    base_url = "https://example.org/slurm/v0.0.44"

    def __init__(self, error: Exception | None, **kwargs):
        self.error = error
        self.kwargs = kwargs

    def ping(self) -> None:
        if self.error is not None:
            raise self.error


class _FakeTokenProvider:
    slurm_username = "alice"

    def get_token(self, force_refresh: bool = False) -> str:
        return "token"


@pytest.mark.parametrize(
    "provider_error,ping_error,expected_message",
    [
        (None, None, None),
        # E.g. no SSO account
        (SlurmTokenError("No SSO account"), None, "No SSO account"),
        # E.g. refresh refused, at the first request
        (None, SlurmTokenError("log in again"), "log in again"),
        (
            None,
            SlurmRestError("GET /ping -> HTTP 502: []"),
            "Cannot reach slurmrestd at https://example.org "
            "(GET /ping -> HTTP 502: [])",
        ),
    ],
)
def test_init_rest_client(
    monkeypatch, provider_error, ping_error, expected_message
):
    """
    Token and `slurmrestd` errors at runner start become job errors (with a
    hint), not tracebacks.
    """

    def _get_token_provider(**kwargs):
        if provider_error is not None:
            raise provider_error
        return _FakeTokenProvider()

    monkeypatch.setattr(
        runner_module, "get_token_provider", _get_token_provider
    )
    monkeypatch.setattr(
        runner_module,
        "SlurmRestClient",
        lambda **kwargs: _FakePingClient(ping_error, **kwargs),
    )
    rest_config = SlurmRestConfig(
        url="https://example.org", ca_bundle="/ca.pem"
    )
    kwargs = dict(resource_name="rest", user_id=1, profile_username="ihbfrac")
    runner = SlurmRestRunner.__new__(SlurmRestRunner)
    if expected_message is None:
        runner._init_rest_client(rest_config=rest_config, **kwargs)
        assert runner.slurm_username == "alice"
        assert runner.rest_client.kwargs == dict(
            base_url="https://example.org",
            username="alice",
            get_token=runner.rest_client.kwargs["get_token"],
            ca_bundle="/ca.pem",
            api_version="v0.0.44",
        )
        rest_config.api_version = "v0.0.44"
        runner._init_rest_client(rest_config=rest_config, **kwargs)
        assert runner.rest_client.kwargs["api_version"] == "v0.0.44"
        return
    with pytest.raises(JobExecutionError) as exc_info:
        runner._init_rest_client(rest_config=rest_config, **kwargs)
    assert expected_message in exc_info.value.assemble_error()


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError(ConnectionRefusedError("refused")),
        # Not `OSError`
        http.client.IncompleteRead(b"partial", expected=10),
    ],
)
def test_client_network_errors(monkeypatch, error):
    """
    Network errors (`OSError`, e.g. connection, DNS, TLS, timeout) and
    malformed responses (`http.client.HTTPException`) become `SlurmRestError`,
    as HTTP errors.
    """

    def fake_urlopen(request, timeout, context):
        raise error

    monkeypatch.setattr(client_module.urllib.request, "urlopen", fake_urlopen)
    client = SlurmRestClient(
        base_url="https://example.org",
        username="user",
        get_token=lambda force_refresh=False: "token",
    )
    with pytest.raises(SlurmRestError) as exc_info:
        client.ping()
    assert str(exc_info.value).startswith(
        f"GET /ping -> {type(error).__name__}: "
    )


@pytest.mark.parametrize(
    "resource_type,backend",
    [
        (ResourceType.LOCAL, local_backend),
        (ResourceType.SLURM_SUDO, slurm_sudo_backend),
        (ResourceType.SLURM_SSH, slurm_ssh_backend),
        (ResourceType.SLURM_REST, slurm_rest_backend),
    ],
)
def test_get_process_workflow(resource_type, backend):
    assert _get_process_workflow(resource_type) is backend.process_workflow


def test_get_process_workflow_unknown():
    with pytest.raises(ValueError, match="Unknown resource type"):
        _get_process_workflow("invalid")
