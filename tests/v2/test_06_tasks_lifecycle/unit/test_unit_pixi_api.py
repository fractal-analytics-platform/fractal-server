import pytest
from fastapi import HTTPException

from fractal_server.app.routes.api.v2.task_collection_pixi import (
    validate_pkgname_and_version,
)


def test_validate_pkgname_and_version():
    with pytest.raises(
        HTTPException,
        match="does not end",
    ):
        validate_pkgname_and_version("something.zip")

    with pytest.raises(
        HTTPException,
        match="a single `-` character",
    ):
        validate_pkgname_and_version("too-many-hyphens.tar.gz")

    pkg_name, version = validate_pkgname_and_version(
        "My_....Nice_Package-0.1.2.3.4.tar.gz"
    )
    assert pkg_name == "my-nice-package"
    assert version == "0.1.2.3.4"
