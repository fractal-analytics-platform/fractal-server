import pytest
from devtools import debug
from fastapi import HTTPException
from httpx import Response
from httpx import TimeoutException

import fractal_server.app.routes.api.v2._aux_functions_task_lifecycle
from fractal_server.app.routes.api.v2._aux_functions_task_lifecycle import (
    get_package_version_from_pypi,
)

PKG = "fractal-tasks-core"


async def test_get_package_version_from_pypi():

    # Success: Provide complete version
    version = await get_package_version_from_pypi(PKG, version="1.2.0")
    debug(version)
    assert version == "1.2.0"

    # Success: Provide incomplete version
    version = await get_package_version_from_pypi(PKG, version="1.2")
    debug(version)
    assert version == "1.2.1"

    # Success: Provide weird incomplete version
    version = await get_package_version_from_pypi(PKG, version="1.2.")
    debug(version)
    assert version == "1.2.1"

    # Success: Check that fractal-tasks-core version is something like `a.b.c`
    actual_latest_version = await get_package_version_from_pypi(PKG)
    debug(actual_latest_version)
    assert actual_latest_version.count(".") == 2

    # Success: use weirdly-normalized name
    version = await get_package_version_from_pypi("FrAcTal-__TaSkS-_-_-CoRe")
    debug(version)
    assert version == actual_latest_version


async def test_get_package_version_from_pypi_failures(monkeypatch):

    # Failure 1: not found
    with pytest.raises(HTTPException, match="status_code 404"):
        await get_package_version_from_pypi(
            "some-very-invalid-task-package-name"
        )

    # Failure 2: invalid incomplete version
    with pytest.raises(HTTPException, match="No version starting"):
        await get_package_version_from_pypi(PKG, version="1.2.2")

    # Failure 3: KeyError due to unexpected response (200, with wrong data)
    async def _patched_get_1(*args, **kwargs):
        return Response(status_code=200, json=dict(key="value"))

    monkeypatch.setattr(
        fractal_server.app.routes.api.v2._aux_functions_task_lifecycle.AsyncClient,  # noqa
        "get",
        _patched_get_1,
    )
    with pytest.raises(HTTPException, match="A KeyError error occurred"):
        await get_package_version_from_pypi(PKG)

    # Failure 4: TimeoutException
    async def _patched_get_2(*args, **kwargs):
        raise TimeoutException("error message")

    monkeypatch.setattr(
        fractal_server.app.routes.api.v2._aux_functions_task_lifecycle.AsyncClient,  # noqa
        "get",
        _patched_get_2,
    )
    with pytest.raises(HTTPException, match="A TimeoutException occurred"):
        await get_package_version_from_pypi(PKG)

    # Failure 5: unknown error
    async def _patched_get_3(*args, **kwargs):
        raise RuntimeError("error message")

    monkeypatch.setattr(
        fractal_server.app.routes.api.v2._aux_functions_task_lifecycle.AsyncClient,  # noqa
        "get",
        _patched_get_3,
    )
    with pytest.raises(HTTPException, match="An unknown error occurred"):
        await get_package_version_from_pypi(PKG)
