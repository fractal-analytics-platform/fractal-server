from fastapi import HTTPException
from fastapi import status

from ....config import get_settings
from ....syringe import Inject


def _backend_supports_shutdown(backend: str) -> bool:
    if backend in ["slurm", "slurm_ssh", "local_experimental"]:
        return True
    else:
        return False


def _check_shutdown_is_supported():
    """
    Raises:
        HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY):
            If FRACTAL_RUNNER_BACKEND is the thread-based 'local' backend.
    """
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND

    if not _backend_supports_shutdown(backend):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Stopping a job execution is not implemented for "
                f"FRACTAL_RUNNER_BACKEND={backend}."
            ),
        )
