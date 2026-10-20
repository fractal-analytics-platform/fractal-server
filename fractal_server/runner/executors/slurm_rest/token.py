import time
from typing import Protocol

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH

from .settings import SlurmRestSettings
from .sso_token import SlurmTokenError
from .sso_token import SsoTokenProvider
from .sso_token import check_token_user

logger = set_logger(__name__)

# A cluster cap (`AuthAltParameters=max_token_lifespan`) makes tokens expire
# earlier: the client then refreshes them on the 401/511 answer
TOKEN_LIFESPAN = 3600


class TokenProvider(Protocol):
    # Token source, for logs
    source: str

    def get_token(self, force_refresh: bool = False) -> str: ...


class ScontrolTokenProvider:
    """
    TEMPORARY: obtain SLURM JWTs via `scontrol token` over SSH, as the
    profile user. To be removed in favour of `SsoTokenProvider`, since it
    needs per-user SSH access.

    Tokens are only kept in memory and refreshed after 80% of their lifespan.
    """

    source = "scontrol (temporary)"

    def __init__(
        self, *, fractal_ssh: FractalSSH, lifespan: int = TOKEN_LIFESPAN
    ) -> None:
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


class StaticTokenProvider:
    """
    TEMPORARY (development only): a fixed token, from
    `FRACTAL_SLURM_REST_TOKEN`. It cannot be refreshed.
    """

    source = "FRACTAL_SLURM_REST_TOKEN (temporary)"

    def __init__(self, *, token: str, slurm_username: str) -> None:
        check_token_user(token, slurm_username)
        self._token = token

    def get_token(self, force_refresh: bool = False) -> str:
        if force_refresh:
            raise SlurmTokenError(
                "slurmrestd rejected FRACTAL_SLURM_REST_TOKEN (expired?)."
            )
        return self._token


def get_token_provider(
    *,
    settings: SlurmRestSettings,
    fractal_ssh: FractalSSH,
    user_id: int,
    slurm_username: str,
) -> TokenProvider:
    """
    Token provider selected by `FRACTAL_SLURM_REST_TOKEN_SOURCE`, unless the
    temporary `FRACTAL_SLURM_REST_TOKEN` override is set.
    """
    provider: TokenProvider
    if settings.FRACTAL_SLURM_REST_TOKEN is not None:
        provider = StaticTokenProvider(
            token=settings.FRACTAL_SLURM_REST_TOKEN.get_secret_value(),
            slurm_username=slurm_username,
        )
    elif settings.FRACTAL_SLURM_REST_TOKEN_SOURCE == "sso":
        provider = SsoTokenProvider(
            user_id=user_id, slurm_username=slurm_username
        )
    else:
        provider = ScontrolTokenProvider(fractal_ssh=fractal_ssh)
    logger.info(
        f"SLURM token source: {provider.source} "
        f"({user_id=}, {slurm_username=})."
    )
    return provider
