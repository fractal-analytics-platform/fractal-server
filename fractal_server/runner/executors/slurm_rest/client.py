import http.client
import json
import ssl
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from fractal_server.logger import set_logger

logger = set_logger(__name__)

# Versions of the `slurmrestd` OpenAPI plugin (`/slurm/<version>/`) that the
# response parsing below and the job description built in
# `job_description.py` work with. All the paths and fields that they use are
# the same in these versions (OpenAPI specs of v0.0.40 to v0.0.44, release
# notes for v0.0.45). Supporting another version may need code changes: check
# the paths and fields used here before adding it.
SUPPORTED_API_VERSIONS = (
    "v0.0.40",
    "v0.0.41",
    "v0.0.42",
    "v0.0.43",
    "v0.0.44",
    "v0.0.45",
)


def squeue_state(job_state: list[str]) -> str:
    """
    Single job state, close to what `squeue --format=%T` reports.

    Args:
        job_state: `job_state` of a `slurmrestd` job.
    """
    # `slurmrestd` returns the base state followed by the flags of a job, e.g.
    # `["COMPLETED", "COMPLETING"]` while the epilog runs, while `squeue`
    # reports a flag instead of the base state. Use last item: a job
    # with flags is not finished yet (except with `SPECIAL_EXIT`, a finished
    # state).
    return job_state[-1] if job_state else "UNKNOWN"


class SlurmRestError(RuntimeError):
    """
    Error of a `slurmrestd` request.

    Attributes:
        status: HTTP status code, if `slurmrestd` answered with an error.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def _parse_body(raw: bytes) -> dict[str, Any]:
    """
    Parse a response body; anything else than a JSON object (e.g. an HTML
    page from a proxy) is returned as `{"raw": <beginning of the body>}`.
    """
    try:
        payload = json.loads(raw)
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        return {"raw": raw.decode(errors="replace")[:500]}
    return payload


def _error_description(err: dict[str, Any]) -> str:
    # `description` is often generic (e.g. "Batch job submission failed"),
    # while `error` has the cause (e.g. "Job cannot be submitted without the
    # current working directory specified.")
    parts = [err.get("description"), err.get("error")]
    text = ": ".join(dict.fromkeys(part for part in parts if part))
    return text or str(err)


def _error_descriptions(payload: dict[str, Any]) -> list[str]:
    descriptions = [
        _error_description(err) for err in payload.get("errors", [])
    ]
    if not descriptions and "raw" in payload:
        descriptions = [payload["raw"]]
    return descriptions


class SlurmRestClient:
    """
    Minimal `slurmrestd` client (`/slurm/<api_version>/` endpoints only).

    Args:
        base_url: e.g. `https://ctrl1.example.org`.
        username: Value of `X-SLURM-USER-NAME`.
        get_token:
            Callable returning a valid JWT; called with `force_refresh=True`
            after a 401 or 511 response.
        ca_bundle:
            Optional CA file (PEM) for TLS verification (else system CAs).
            Needed only when the system CAs are insufficient to verify the
            `slurmrestd` server's TLS certificate (eg. local dev).
        api_version:
            `slurmrestd` OpenAPI version (one of `SUPPORTED_API_VERSIONS`).
        timeout: Timeout of each request, in seconds.
    """

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        get_token: Callable[..., str],
        ca_bundle: str | None = None,
        api_version: str,
        timeout: float = 30.0,
    ) -> None:
        # Tokens are sent in request headers
        if not base_url.startswith("https://"):
            raise ValueError(
                f"Invalid slurmrestd URL scheme (https only): '{base_url}'."
            )
        if api_version not in SUPPORTED_API_VERSIONS:
            raise ValueError(
                f"Unsupported slurmrestd API version '{api_version}' "
                f"(supported: {list(SUPPORTED_API_VERSIONS)})."
            )
        self.root_url = base_url.rstrip("/")
        self.api_version = api_version
        self.base_url = f"{self.root_url}/slurm/{api_version}"
        self.username = username
        self.get_token = get_token
        self.ca_bundle = ca_bundle
        self.timeout = timeout
        try:
            self._ssl_context = ssl.create_default_context(cafile=ca_bundle)
        except (OSError, ssl.SSLError) as e:
            raise SlurmRestError(
                f"Cannot load CA bundle '{ca_bundle}': {e}"
            ) from None

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        for attempt in (1, 2):
            request = urllib.request.Request(
                url,
                method=method,
                data=data,
                headers={
                    "X-SLURM-USER-NAME": self.username,
                    "X-SLURM-USER-TOKEN": self.get_token(
                        force_refresh=(attempt == 2)
                    ),
                    "Content-Type": "application/json",
                },
            )
            try:
                # `https` scheme is checked in `__init__`
                with urllib.request.urlopen(  # nosec B310
                    request, timeout=self.timeout, context=self._ssl_context
                ) as response:
                    payload = _parse_body(response.read())
            except urllib.error.HTTPError as e:
                payload = _parse_body(e.read())
                # slurmrestd answers 511 for invalid or expired tokens (on
                # Slurm 23.11, 500: not retried here, since 500 is also used
                # for other errors; SSO tokens are refreshed before they
                # expire anyway)
                if e.code in (401, 511) and attempt == 1:
                    logger.warning(
                        f"{method} {path} -> {e.code}, refreshing token."
                    )
                    continue
                raise SlurmRestError(
                    f"{method} {path} -> HTTP {e.code}: "
                    f"{_error_descriptions(payload)}",
                    status=e.code,
                ) from None
            except (OSError, http.client.HTTPException) as e:
                # Connection, DNS, TLS and timeout errors (after `HTTPError`,
                # which is an `OSError` too), and malformed or truncated
                # responses (`http.client` errors that are not `OSError`)
                raise SlurmRestError(
                    f"{method} {path} -> {type(e).__name__}: {e}"
                ) from None
            if "raw" in payload:
                raise SlurmRestError(
                    f"{method} {path} -> response is not a JSON object: "
                    f"{payload['raw']!r}"
                )
            if payload.get("errors"):
                raise SlurmRestError(
                    f"{method} {path} -> errors: {_error_descriptions(payload)}"
                )
            for warning in payload.get("warnings", []):
                logger.warning(f"{method} {path} -> warning: {warning}")
            return payload
        raise SlurmRestError(f"{method} {path} -> unexpected retry exit.")

    def ping(self) -> None:
        self._request("GET", "/ping")

    def served_api_versions(self) -> list[str]:
        """
        Supported API versions that `slurmrestd` serves, i.e. whose `/ping`
        succeeds (a version that is not served answers 404).
        """
        served = []
        for api_version in SUPPORTED_API_VERSIONS:
            client = SlurmRestClient(
                base_url=self.root_url,
                username=self.username,
                get_token=self.get_token,
                ca_bundle=self.ca_bundle,
                api_version=api_version,
                timeout=self.timeout,
            )
            try:
                client.ping()
            except SlurmRestError:
                continue
            served.append(api_version)
        return served

    def submit_job(self, request_body: dict[str, Any]) -> int:
        """
        Submit a batch job and return its SLURM job ID.

        Args:
            request_body: Dictionary with `script` and `job` keys.
        """
        payload = self._request("POST", "/job/submit", body=request_body)
        job_id = payload.get("job_id")
        # slurmrestd accepts the job and only warns about unknown fields, so
        # a wrong mapping in `job_description.py` would silently drop options
        unknown_fields = [
            warning.get("description")
            for warning in payload.get("warnings", [])
            if "unknown field" in str(warning.get("description")).lower()
        ]
        if unknown_fields:
            message = f"Job description has unknown fields: {unknown_fields}"
            if job_id is not None:
                # Do not hide the original error if the cleanup fails
                try:
                    self.cancel_job(str(job_id))
                except Exception as e:
                    message += (
                        f" (job {job_id} may still be queued, cancel "
                        f"failed: {e})"
                    )
            raise SlurmRestError(message)
        if job_id is None:
            raise SlurmRestError(f"No `job_id` in submit response: {payload}")
        return int(job_id)

    def list_jobs(self) -> list[dict[str, Any]]:
        """
        Return the jobs visible to the user (`GET /jobs` wrapper), as `squeue`
        sees them: pending and running jobs, and jobs that finished less than
        `MinJobAge` seconds ago (slurm config); older finished jobs are only
        in the accounting database, and are not listed.
        """
        payload = self._request("GET", "/jobs")
        # A missing job is considered completed upstream: a response without
        # a job list must fail, not mean "no jobs"
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise SlurmRestError(
                f"GET /jobs -> no `jobs` list in response (keys: "
                f"{sorted(payload)})."
            )
        return jobs

    def get_job_states(self) -> dict[str, str]:
        """
        Return `{job_id: state}` for all jobs visible to the user.

        NOTE: Jobs that finished more than `MinJobAge` seconds ago are not
        listed (see `list_jobs`) -> treat missing jobs as completed.
        """
        states = {}
        for job in self.list_jobs():
            job_state = job.get("job_state")
            # `job_state` is a list (base state, then flags),
            # e.g. ["COMPLETED", "COMPLETING"]
            if isinstance(job_state, list):
                job_state = squeue_state(job_state)
            states[str(job["job_id"])] = job_state
        return states

    def cancel_job(self, job_id: str) -> None:
        self._request("DELETE", f"/job/{job_id}")
