from typing import Protocol

from fractal_server.logger import set_logger

from .settings import SlurmRestSettings
from .sso_token import SlurmTokenError
from .sso_token import SsoTokenProvider
from .sso_token import jwt_claims

logger = set_logger(__name__)


class TokenProvider(Protocol):
    # Token source, for logs
    source: str
    # User that `slurmrestd` acts as (the token user)
    slurm_username: str

    def get_token(self, force_refresh: bool = False) -> str: ...


class StaticTokenProvider:
    """
    Development only: a fixed token, from `FRACTAL_SLURM_REST_TOKEN`, used
    for all Fractal users. It cannot be refreshed. The SLURM user is the
    token user (`sub` or `sun` claim), else `default_username`.
    """

    source = "FRACTAL_SLURM_REST_TOKEN (development only)"

    def __init__(self, *, token: str, default_username: str) -> None:
        claims = jwt_claims(token)
        self.slurm_username: str = claims.get(
            "sub", claims.get("sun", default_username)
        )
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
    user_id: int,
    profile_username: str,
) -> TokenProvider:
    """
    SSO token provider of the user, unless the development-only
    `FRACTAL_SLURM_REST_TOKEN` override is set.

    Args:
        settings:
        user_id: Fractal user.
        profile_username:
            SSH user; also the SLURM user of `FRACTAL_SLURM_REST_TOKEN`, if
            the token has no user claim.
    """
    provider: TokenProvider
    if settings.FRACTAL_SLURM_REST_TOKEN is not None:
        provider = StaticTokenProvider(
            token=settings.FRACTAL_SLURM_REST_TOKEN.get_secret_value(),
            default_username=profile_username,
        )
        logger.warning(
            "FRACTAL_SLURM_REST_TOKEN is set: all SLURM requests use this "
            f"token, as SLURM user '{provider.slurm_username}' (development "
            "only, do not use in deployments)."
        )
    else:
        provider = SsoTokenProvider(user_id=user_id)
    logger.info(
        f"SLURM token source: {provider.source} ({user_id=}, "
        f"ssh user={profile_username}, slurm user={provider.slurm_username})."
    )
    return provider
