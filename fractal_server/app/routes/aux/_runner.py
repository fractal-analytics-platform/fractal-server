from fastapi import HTTPException
from fastapi import status

from ....config import get_settings
from ....syringe import Inject


def _check_backend_is_slurm():
    """
    Raises:
        HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY):
            If FRACTAL_RUNNER_BACKEND is not 'slurm'
    """
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND
    if backend != "slurm":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Stopping a job execution is not implemented for "
                f"FRACTAL_RUNNER_BACKEND={backend}."
            ),
        )
