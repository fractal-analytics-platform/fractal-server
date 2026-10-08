import time

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH

logger = set_logger(__name__)


class SlurmTokenProvider:
    """
    Obtain SLURM JWTs via `scontrol token` over SSH, as the profile user.

    Tokens are only kept in memory and refreshed after 80% of their lifespan.
    """

    def __init__(self, *, fractal_ssh: FractalSSH, lifespan: int) -> None:
        self.fractal_ssh = fractal_ssh
        self.lifespan = lifespan
        self._token: str | None = None
        self._refresh_at: float = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
        now = time.monotonic()
        if force_refresh or self._token is None or now >= self._refresh_at:
            self._token = self._fetch_token()
            self._refresh_at = now + 0.8 * self.lifespan
            logger.info(f"Obtained new SLURM token ({self.lifespan=} s).")
        return self._token

    def _fetch_token(self) -> str:
        # `FractalSSH.run_command` logs stdout, which would leak the token.
        result = self.fractal_ssh._run(
            f"scontrol token lifespan={self.lifespan}",
            label="scontrol token",
            hide=True,
            in_stream=False,
        )
        for line in result.stdout.splitlines():
            if line.startswith("SLURM_JWT="):
                return line.split("=", 1)[1].strip()
        raise RuntimeError("`scontrol token` output has no `SLURM_JWT=` line.")
