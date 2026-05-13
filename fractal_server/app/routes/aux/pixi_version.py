from fastapi import HTTPException
from fastapi import status

from fractal_server.app.models import Resource


def check_pixi_version(
    *,
    pixi_version: str,
    resource: Resource,
) -> None:
    """
    Verify that the requested pixi version is available for this resource.

    Args:
        pixi_version:
        resource:
    """
    if not resource.tasks_pixi_config:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Pixi task collection is not available.",
        )
    elif pixi_version not in resource.tasks_pixi_config["versions"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Pixi version '{pixi_version}' is not available. "
                "Available versions: "
                f"{list(resource.tasks_pixi_config['versions'].keys())}"
            ),
        )
