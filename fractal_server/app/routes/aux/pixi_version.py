from fastapi import HTTPException
from fastapi import status

from fractal_server.app.models import Resource


def get_pixi_version(
    *,
    pixi_version: str | None,
    resource: Resource,
) -> None:
    """
    Get valid pixi version based on resource configuration.

    Args:
        pixi_version: If `None`, return the default version.
        resource:
    """
    if not resource.tasks_pixi_config:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Pixi task collection is not available.",
        )
    if pixi_version is None:
        return resource.tasks_pixi_config["default_version"]
    else:
        if pixi_version not in resource.tasks_pixi_config["versions"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"Pixi version '{pixi_version}' is not available. "
                    "Available versions: "
                    f"{list(resource.tasks_pixi_config['versions'].keys())}"
                ),
            )
        return pixi_version
