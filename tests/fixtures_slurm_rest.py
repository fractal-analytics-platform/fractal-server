"""
Fixtures to talk to the real `slurmrestd` of the SLURM docker container.

The container runs `slurmrestd` (Slurm 23.11, API `v0.0.40`, JWT auth) behind
a TLS proxy on port 6820, published on the host. The server certificate is
valid for `localhost` (not for the container IP), and is signed by a test CA
available in the container.
"""

import logging
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError

SLURM_REST_URL = "https://localhost:6820"
SLURM_REST_API_VERSION = "v0.0.40"
SLURM_REST_CA_PATH = "/etc/slurm/tls/ca.pem"


@pytest.fixture
def slurm_rest_ca_bundle(run_in_container, tmp_path: Path) -> str:
    """
    Absolute path of a local copy of the test CA of the container.
    """
    res = run_in_container(f"cat {SLURM_REST_CA_PATH}")
    if res.returncode != 0 or "BEGIN CERTIFICATE" not in res.stdout:
        raise RuntimeError(
            f"Cannot read {SLURM_REST_CA_PATH} in the SLURM container "
            f"(image without slurmrestd?): {res.stderr}"
        )
    ca_bundle = tmp_path / "slurmrestd-ca.pem"
    ca_bundle.write_text(res.stdout)
    return ca_bundle.as_posix()


@pytest.fixture
def slurm_rest_token(run_in_container, ssh_username: str) -> Callable[..., str]:
    """
    Factory of SLURM tokens (`scontrol token`) for a user of the container.
    """

    def _token(username: str = ssh_username, lifespan: int = 3600) -> str:
        res = run_in_container(
            f"scontrol token username={username} lifespan={lifespan}"
        )
        prefix = "SLURM_JWT="
        if res.returncode != 0 or not res.stdout.startswith(prefix):
            raise RuntimeError(
                f"Cannot create a SLURM token for '{username}': {res.stderr}"
            )
        return res.stdout.strip().removeprefix(prefix)

    return _token


@pytest.fixture
def slurm_rest_alive(
    slurm_alive,
    slurm_rest_ca_bundle: str,
    slurm_rest_token: Callable[..., str],
    ssh_username: str,
) -> None:
    """
    Wait until `slurmrestd` answers `GET /ping` through the TLS proxy.
    """
    token = slurm_rest_token()
    client = SlurmRestClient(
        base_url=SLURM_REST_URL,
        username=ssh_username,
        get_token=lambda force_refresh=False: token,
        ca_bundle=slurm_rest_ca_bundle,
        api_version=SLURM_REST_API_VERSION,
        timeout=5.0,
    )
    max_attempts = 60
    interval = 0.5
    for attempt in range(max_attempts):
        try:
            client.ping()
            logging.info("[slurm_rest_alive] slurmrestd is up.")
            return
        except SlurmRestError as e:
            logging.info(
                f"[slurm_rest_alive] Attempt {attempt + 1}/{max_attempts}: {e}"
            )
            time.sleep(interval)
    raise RuntimeError(
        f"[slurm_rest_alive] slurmrestd not reachable at {SLURM_REST_URL}."
    )
