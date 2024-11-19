import json

import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCollectCustomV2
from fractal_server.app.schemas.v2 import TaskCollectPipV2
from fractal_server.app.schemas.v2 import TaskGroupCreateV2Strict
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum


def test_TaskCollectPipV2():
    """
    Check that leading/trailing whitespace characters were removed
    """
    collection = TaskCollectPipV2(
        package="  package  ",
        package_version="  1.2.3  ",
    )
    assert collection.package == "package"
    assert collection.package_version == "1.2.3"

    collection_none = TaskCollectPipV2(
        package="pkg", pinned_package_versions=None
    )
    assert collection_none.pinned_package_versions is None

    sanitized_keys = TaskCollectPipV2(
        package="pkg", pinned_package_versions={"    a      ": "1.0.0"}
    )
    assert sanitized_keys.pinned_package_versions == dict(a="1.0.0")

    with pytest.raises(
        ValidationError, match="Local-package path must be absolute"
    ):
        TaskCollectPipV2(package="not/absolute.whl")

    with pytest.raises(ValidationError):
        TaskCollectPipV2(
            package="pkg", pinned_package_versions={";maliciouscmd": "1.0.0"}
        )

    with pytest.raises(ValidationError):
        TaskCollectPipV2(
            package="pkg", pinned_package_versions={"pkg": ";maliciouscmd"}
        )

    with pytest.raises(ValidationError):
        TaskCollectPipV2(
            package="pkg",
            pinned_package_versions={" a ": "1.0.0", "a": "2.0.0"},
        )

    with pytest.raises(ValidationError, match="package must not contain"):
        TaskCollectPipV2(package="; rm x")

    with pytest.raises(
        ValidationError, match="package_version must not contain"
    ):
        TaskCollectPipV2(package="pkg", package_version="; rm x")

    with pytest.raises(
        ValidationError, match="package_extras must not contain"
    ):
        TaskCollectPipV2(
            package="pkg", package_version="1.2.3", package_extras="]; rm x; ["
        )


async def test_TaskCollectCustomV2(testdata_path):

    manifest_file = (
        testdata_path.parent
        / "v2/fractal_tasks_mock"
        / "src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json"
    ).as_posix()

    with open(manifest_file, "r") as f:
        manifest_dict = json.load(f)

    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="b",
            package_root=None,
            package_name="inject;code",
        )
    assert "must not contain" in e._excinfo[1].errors()[0]["msg"]

    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="a",
            label="name",
            package_root=None,
            package_name="name",
        )
    assert (
        "Python interpreter path must be absolute"
        in e._excinfo[1].errors()[0]["msg"]
    )

    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="name",
            package_root="non_absolute_path",
            package_name=None,
        )
    assert "'package_root' must be an absolute path" in str(e.value)

    # Fail because neither 'package_root' nor 'package_name'
    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="name",
            package_root=None,
            package_name=None,
            version=None,
        )
    assert "One and only one must be set" in str(e.value)

    # Successful
    collection = TaskCollectCustomV2(
        manifest=ManifestV2(**manifest_dict),
        python_interpreter="  /some/python                  ",
        label="b",
        package_root="  /somewhere  ",
        package_name=None,
    )
    # Check that trailing whitespace characters were removed
    assert collection.python_interpreter == "/some/python"
    assert collection.package_root == "/somewhere"


def test_TaskGroupCreateV2Strict():
    # Success
    TaskGroupCreateV2Strict(
        path="/a",
        venv_path="/b",
        version="/c",
        python_version="/d",
        origin=TaskGroupV2OriginEnum.WHEELFILE,
        wheel_path="/a",
        pkg_name="x",
        user_id=1,
    )
    # Validators from parent class
    with pytest.raises(ValueError, match="absolute path"):
        TaskGroupCreateV2Strict(
            path="a",
            venv_path="b",
            version="c",
            python_version="d",
            origin=TaskGroupV2OriginEnum.PYPI,
            pkg_name="x",
            user_id=1,
        )
    # No path
    with pytest.raises(ValidationError):
        TaskGroupCreateV2Strict(
            venv_path="/b",
            version="c",
            python_version="d",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path="/a",
            pkg_name="x",
            user_id=1,
        )
    # No venv_path
    with pytest.raises(ValidationError):
        TaskGroupCreateV2Strict(
            path="/a",
            version="c",
            python_version="d",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path="/a",
            pkg_name="x",
            user_id=1,
        )
    # No version
    with pytest.raises(ValidationError):
        TaskGroupCreateV2Strict(
            path="/a",
            venv_path="/b",
            python_version="d",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path="/a",
            pkg_name="x",
            user_id=1,
        )
    # No python_version
    with pytest.raises(ValidationError):
        TaskGroupCreateV2Strict(
            path="/a",
            venv_path="/b",
            version="c",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path="/a",
            pkg_name="x",
            user_id=1,
        )
    # Wheel path set for pypi origin
    with pytest.raises(ValueError, match="origin"):
        TaskGroupCreateV2Strict(
            path="/a",
            venv_path="/b",
            version="c",
            python_version="d",
            origin=TaskGroupV2OriginEnum.PYPI,
            wheel_path="/a",
            pkg_name="x",
            user_id=1,
        )
    # Wheel path unset for wheel origin
    with pytest.raises(ValueError, match="origin"):
        TaskGroupCreateV2Strict(
            path="/a",
            venv_path="/b",
            version="c",
            python_version="d",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path=None,
            pkg_name="x",
            user_id=1,
        )
