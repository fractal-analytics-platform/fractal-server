import http.client
import io
import json
import urllib.error

import pytest

from fractal_server.runner.executors.slurm_common._job_states import (
    STATES_FINISHED,
)
from fractal_server.runner.executors.slurm_rest import client as client_module
from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.client import squeue_state


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
        api_version="v0.0.44",
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
    # Errors in an HTTP 200 response
    responses.append({"errors": [{"error": "bad"}]})
    with pytest.raises(SlurmRestError, match="bad"):
        client.cancel_job("1")

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
        api_version="v0.0.44",
        get_token=lambda force_refresh=False: "token",
    )
    with pytest.raises(SlurmRestError):
        client.get_job_states()

    # An empty job list is valid: jobs are missing, hence completed
    monkeypatch.setattr(
        client_module.urllib.request,
        "urlopen",
        lambda request, timeout, context: _FakeResponse(b'{"jobs": []}'),
    )
    assert client.get_job_states() == {}


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
            api_version="v0.0.44",
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
            api_version="v0.0.44",
            get_token=lambda force_refresh=False: "token",
            ca_bundle=(tmp_path / "missing.pem").as_posix(),
        )


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
        api_version="v0.0.44",
        get_token=lambda force_refresh=False: "token",
    )
    with pytest.raises(SlurmRestError) as exc_info:
        client.ping()
    assert str(exc_info.value).startswith(
        f"GET /ping -> {type(error).__name__}: "
    )
