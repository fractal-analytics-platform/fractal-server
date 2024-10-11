from typing import Optional

from fastapi import HTTPException
from fastapi import status
from httpx import AsyncClient
from httpx import TimeoutException

from fractal_server.logger import set_logger


logger = set_logger(__name__)


async def get_package_version_from_pypi(
    name: str,
    version: Optional[str] = None,
) -> str:
    """
    Make a GET call to PyPI JSON API and get latest *compatible* version.

    There are three cases:

    1. `version` is set and it is found on PyPI as-is.
    2. `version` is set but it is not found on PyPI as-is.
    3. `version` is unset, and we query `PyPI` for latest.

    Ref https://warehouse.pypa.io/api-reference/json.html.

    Arguments:
        name: Package name.
        version:
            Could be a correct version (`1.3.0`), an incomplete one
            (`1.3`) or `None`.
    """

    url = f"https://pypi.org/pypi/{name}/json"
    hint = f"Hint: specify the required version for '{name}'."

    # Make request to PyPI
    try:
        async with AsyncClient(timeout=5.0) as client:
            res = await client.get(url)
    except TimeoutException as e:
        error_msg = (
            f"A TimeoutException occurred while getting {url}.\n"
            f"Original error: {str(e)}."
        )
        logger.error(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )
    except BaseException as e:
        error_msg = (
            f"An unknown error occurred while getting {url}. "
            f"Original error: {str(e)}."
        )
        logger.error(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )

    # Parse response
    if res.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Could not get {url} (status_code {res.status_code})."
                f"\n{hint}"
            ),
        )
    try:
        response_data = res.json()
        latest_version = response_data["info"]["version"]
        available_releases = response_data["releases"].keys()
    except KeyError as e:
        logger.error(
            f"A KeyError occurred while getting {url}. "
            f"Original error: {str(e)}."
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"A KeyError error occurred while getting {url}.\n{hint}",
        )

    logger.info(
        f"Obtained data from {url}: "
        f"{len(available_releases)} releases, "
        f"latest={latest_version}."
    )

    if version is not None:
        if version in available_releases:
            logger.info(f"Requested {version=} available on PyPI.")
            # Case 1: `version` is set and it is found on PyPI as-is
            return version
        else:
            # Case 2: `version` is set but it is not found on PyPI as-is
            # Filter using `version` as prefix, and sort
            matching_versions = [
                v for v in available_releases if v.startswith(version)
            ]
            logger.info(
                f"Requested {version=} not available on PyPI, "
                f"found {len(matching_versions)} versions matching "
                f"`{version}*`."
            )
            if len(matching_versions) == 0:
                logger.info(f"No version starting with {version} found.")
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"No version starting with {version} found.\n"
                        f"{hint}"
                    ),
                )
            else:
                latest_matching_version = sorted(matching_versions)[-1]
                return latest_matching_version
    else:
        # Case 3: `version` is unset and we use latest
        logger.info(f"No version requested, returning {latest_version=}.")
        return latest_version
