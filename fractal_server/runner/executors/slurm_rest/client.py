import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from fractal_server.logger import set_logger

logger = set_logger(__name__)


class SlurmRestError(RuntimeError):
    pass


def _parse_body(raw: bytes) -> dict[str, Any]:
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw.decode(errors="replace")[:500]}


def _error_descriptions(payload: dict[str, Any]) -> list[str]:
    descriptions = [
        err.get("description") or err.get("error") or str(err)
        for err in payload.get("errors", [])
    ]
    if not descriptions and "raw" in payload:
        descriptions = [payload["raw"]]
    return descriptions


class SlurmRestClient:
    """
    Minimal `slurmrestd` client (`/slurm/<api_version>/` endpoints only).

    Args:
        base_url: e.g. `https://ctrl1.example.org`.
        api_version: e.g. `v0.0.44`.
        username: Value of `X-SLURM-USER-NAME`.
        get_token:
            Callable returning a valid JWT; called with `force_refresh=True`
            after a 401 or 511 response.
        verify_tls: Whether to verify the server TLS certificate.
        ca_bundle: Optional CA file (PEM) used for TLS verification.
        timeout: Timeout of each request, in seconds.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_version: str,
        username: str,
        get_token: Callable[..., str],
        verify_tls: bool = True,
        ca_bundle: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = f"{base_url.rstrip('/')}/slurm/{api_version}"
        self.slurmdb_url = f"{base_url.rstrip('/')}/slurmdb/{api_version}"
        self.username = username
        self.get_token = get_token
        self.timeout = timeout
        if verify_tls:
            self._ssl_context = ssl.create_default_context(cafile=ca_bundle)
        else:
            logger.warning("TLS verification is disabled for slurmrestd.")
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        slurmdb: bool = False,
    ) -> dict[str, Any]:
        url = f"{self.slurmdb_url if slurmdb else self.base_url}{path}"
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
                with urllib.request.urlopen(
                    request, timeout=self.timeout, context=self._ssl_context
                ) as response:
                    payload = _parse_body(response.read())
            except urllib.error.HTTPError as e:
                payload = _parse_body(e.read())
                # slurmrestd answers 511 for invalid or expired tokens
                if e.code in (401, 511) and attempt == 1:
                    logger.info(
                        f"{method} {path} -> {e.code}, refreshing token."
                    )
                    continue
                raise SlurmRestError(
                    f"{method} {path} -> HTTP {e.code}: "
                    f"{_error_descriptions(payload)}"
                ) from None
            errors = _error_descriptions(payload)
            if payload.get("errors"):
                raise SlurmRestError(f"{method} {path} -> errors: {errors}")
            for warning in payload.get("warnings", []):
                logger.warning(f"{method} {path} -> warning: {warning}")
            return payload
        raise SlurmRestError(f"{method} {path} -> unexpected retry exit.")

    def ping(self) -> None:
        self._request("GET", "/ping")

    def submit_job(self, request_body: dict[str, Any]) -> int:
        """
        Submit a batch job and return its SLURM job ID.

        Args:
            request_body: Dictionary with `script` and `job` keys.
        """
        payload = self._request("POST", "/job/submit", body=request_body)
        job_id = payload.get("job_id")
        if job_id is None:
            raise SlurmRestError(f"No `job_id` in submit response: {payload}")
        return int(job_id)

    def get_job_states(self) -> dict[str, str]:
        """
        Return `{job_id: state}` for all jobs visible to the user.

        Note: `GET /job/{id1},{id2}` is not supported, hence the single
        `GET /jobs` call. Jobs older than `MinJobAge` are not listed.
        """
        payload = self._request("GET", "/jobs")
        states = {}
        for job in payload.get("jobs", []):
            job_state = job.get("job_state")
            # v0.0.44 returns a list, e.g. ["COMPLETED", "COMPLETING"]
            if isinstance(job_state, list):
                job_state = job_state[0] if job_state else "UNKNOWN"
            states[str(job["job_id"])] = job_state
        return states

    def cancel_job(self, job_id: str) -> None:
        self._request("DELETE", f"/job/{job_id}")

    def list_jobs(self) -> list[dict[str, Any]]:
        """
        Return all jobs visible to the user (raw `GET /jobs` items).
        """
        return self._request("GET", "/jobs").get("jobs", [])

    def list_accounting_jobs(self, *, start_time: int) -> list[dict[str, Any]]:
        """
        Return the user's jobs from `slurmdbd`, started after `start_time`.

        Note: `comment` is only filled once a job has ended.

        Args:
            start_time: Unix timestamp.
        """
        query = urllib.parse.urlencode(
            dict(
                users=self.username,
                start_time=start_time,
                skip_steps="true",
            )
        )
        return self._request("GET", f"/jobs?{query}", slurmdb=True).get(
            "jobs", []
        )
