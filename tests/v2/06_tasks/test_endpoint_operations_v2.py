import pytest
from devtools import debug
from fastapi import HTTPException
from httpx import Response

import fractal_server.tasks.v2.endpoint_operations
from fractal_server.tasks.v2.endpoint_operations import (
    get_package_version_from_pypi,
)


async def test_get_package_version_from_pypi(monkeypatch):

    # Success: Check that fractal-tasks-core version is something like `a.b.c`
    version = await get_package_version_from_pypi("fractal-tasks-core")
    debug(version)
    assert version.count(".") == 2

    # Success: use weirdly-normalized name
    new_version = await get_package_version_from_pypi(
        "FrAcTal-__TaSkS-_-_-CoRe"
    )
    debug(new_version)
    assert new_version == version

    # Failure 1: not found

    with pytest.raises(HTTPException, match="status_code 404"):
        await get_package_version_from_pypi(
            "some-very-invalid-task-package-name"
        )

    # Failure 2: KeyError due to unexpected response

    async def _patched_get(*args, **kwargs):
        return Response(status_code=200, json=dict(key="value"))

    monkeypatch.setattr(
        fractal_server.tasks.v2.endpoint_operations.AsyncClient,
        "get",
        _patched_get,
    )
    with pytest.raises(HTTPException, match="An error occurred"):
        await get_package_version_from_pypi("fractal-tasks-core")
