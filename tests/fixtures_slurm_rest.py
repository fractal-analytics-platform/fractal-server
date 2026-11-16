"""
Fixtures to talk to the real `slurmrestd` of the SLURM docker container.

The container (`ghcr.io/fractal-analytics-platform/ubuntu24-slurm`, see
fractal-containers `images/ubuntu24-slurm/`) runs `slurmrestd` (Slurm 23.11,
API `v0.0.40`, JWT auth) behind a TLS proxy on port 6820, published on the
host. The server certificate is valid for `localhost` (not for the container
IP), and is signed by a test CA, whose public certificate is copied in
`tests/data/slurm_docker_images/slurmrestd_ca.pem` (from fractal-containers
`images/ubuntu24-slurm/tls/ca.pem`: update it if that one changes). JWTs are
signed with a fixed test-only key of the container.
"""

import base64
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError

SLURM_REST_URL = "https://localhost:6820"
SLURM_REST_API_VERSION = "v0.0.40"
# Test-only HS256 key of the container (`/etc/slurm/jwt_hs256.key`)
SLURM_REST_JWT_KEY = b"fractal-containers-test-only-jwt-key"


def make_slurm_rest_token(username: str, lifespan: int = 3600) -> str:
    """
    SLURM token (HS256 JWT, as made by `scontrol token`) for a user of the
    container.
    """

    def _b64(data: bytes) -> bytes:
        return base64.urlsafe_b64encode(data).rstrip(b"=")

    now = int(time.time())
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64(
        json.dumps(
            {"sun": username, "iat": now, "exp": now + lifespan}
        ).encode()
    )
    signature = _b64(
        hmac.new(
            SLURM_REST_JWT_KEY, header + b"." + payload, hashlib.sha256
        ).digest()
    )
    return (header + b"." + payload + b"." + signature).decode()


@pytest.fixture
def slurm_rest_ca_bundle(testdata_path: Path) -> str:
    """
    Absolute path of the test CA of the container.
    """
    path = testdata_path / "slurm_docker_images" / "slurmrestd_ca.pem"
    if not path.is_file():
        raise FileNotFoundError(
            f"Missing {path}: copy it from fractal-containers "
            "`images/ubuntu24-slurm/tls/ca.pem`."
        )
    return path.absolute().as_posix()


@pytest.fixture
def slurm_rest_token(ssh_username: str) -> Callable[..., str]:
    """
    Factory of SLURM tokens for users of the container (default: the SSH
    user).
    """

    def _token(username: str = ssh_username, lifespan: int = 3600) -> str:
        return make_slurm_rest_token(username=username, lifespan=lifespan)

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
