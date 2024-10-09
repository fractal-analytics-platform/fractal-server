from fastapi import HTTPException
from fastapi import status
from httpx import AsyncClient
from httpx import TimeoutException

from fractal_server.logger import set_logger


logger = set_logger(__name__)


async def get_package_version_from_pypi(name: str) -> str:
    """
    Make a GET call to PyPI JSON API and get latest package version.

    Ref https://warehouse.pypa.io/api-reference/json.html.

    Arguments:
        name: Package name.
    """

    url = f"https://pypi.org/pypi/{name}/json"
    hint = f"Hint: specify the required version for '{name}'."
    try:
        async with AsyncClient(timeout=5.0) as client:
            res = await client.get(url)
            if res.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Could not get {url} (status_code {res.status_code})."
                        f"\n{hint}"
                    ),
                )
            version = res.json()["info"]["version"]
            return version
    except (KeyError, TimeoutException) as e:
        logger.warning(
            f"An error occurred while getting {url}. Original error: {str(e)}."
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"An error occurred while getting {url}.\n{hint}"),
        )
