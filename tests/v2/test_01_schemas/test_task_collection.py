import json

import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCollectCustom
from fractal_server.app.schemas.v2 import TaskCollectPip
from fractal_server.app.schemas.v2 import TaskGroupCreateStrict
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum


def test_TaskCollectPipV2():
    """
    Check that leading/trailing whitespace characters were removed
    """
    collection = TaskCollectPip(
        package="  package  ",
        package_version="  1.2.3  ",
    )
    assert collection.package == "package"
    assert collection.package_version == "1.2.3"

    collection_none = TaskCollectPip(
        package="pkg", pinned_package_versions_post=None
    )
    assert collection_none.pinned_package_versions_post is None

    sanitized_keys = TaskCollectPip(
        package="pkg", pinned_package_versions_post={"    a      ": "1.0.0"}
    )
    assert sanitized_keys.pinned_package_versions_post == dict(a="1.0.0")

    with pytest.raises(ValidationError):
        TaskCollectPip(
            package="pkg",
            pinned_package_versions_post={";maliciouscmd": "1.0.0"},
        )

    with pytest.raises(ValidationError):
        TaskCollectPip(
            package="pkg",
            pinned_package_versions_post={"pkg": ";maliciouscmd"},
        )

    with pytest.raises(ValidationError):
        TaskCollectPip(
            package="pkg",
            pinned_package_versions_post={" a ": "1.0.0", "a": "2.0.0"},
        )
    with pytest.raises(ValidationError):
        TaskCollectPip(
            package="pkg", pinned_package_versions_post={" ": "1.0.0"}
        )

    with pytest.raises(ValidationError, match="must not contain"):
        TaskCollectPip(package="; rm x")

    with pytest.raises(ValidationError, match="must not contain"):
        TaskCollectPip(package="pkg", package_version="; rm x")

    with pytest.raises(ValidationError, match="must not contain"):
        TaskCollectPip(
            package="pkg", package_version="1.2.3", package_extras="]; rm x; ["
        )


async def test_TaskCollectCustomV2(testdata_path):
    manifest_file = (
        testdata_path.parent
        / "v2/fractal_tasks_mock"
        / "src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json"
    ).as_posix()

    with open(manifest_file) as f:
        manifest_dict = json.load(f)

    with pytest.raises(ValidationError) as e:
        TaskCollectCustom(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="b",
            package_root=None,
            package_name="inject;code",
        )
    assert "must not contain" in e._excinfo[1].errors()[0]["msg"]

    with pytest.raises(ValidationError) as e:
        TaskCollectCustom(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="a",
            label="name",
            package_root=None,
            package_name="name",
        )
    assert "String must be an absolute path" in str(e.value)

    with pytest.raises(ValidationError) as e:
        TaskCollectCustom(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="name",
            package_root="non_absolute_path",
            package_name=None,
        )
    assert "String must be an absolute path" in str(e.value)

    # Fail because neither 'package_root' nor 'package_name'
    with pytest.raises(ValidationError) as e:
        TaskCollectCustom(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            label="name",
            package_root=None,
            package_name=None,
            version=None,
        )
    assert "One and only one must be set" in str(e.value)

    # Successful
    collection = TaskCollectCustom(
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
    TaskGroupCreateStrict(
        path="/a",
        venv_path="/b",
        version="/c",
        python_version="/d",
        origin=TaskGroupOriginEnum.WHEELFILE,
        archive_path="/a",
        pkg_name="x",
        user_id=1,
        resource_id=1,
    )
    # Validators from parent class
    with pytest.raises(ValueError, match="absolute path"):
        TaskGroupCreateStrict(
            path="a",
            venv_path="b",
            version="c",
            python_version="d",
            origin=TaskGroupOriginEnum.PYPI,
            pkg_name="x",
            user_id=1,
            resource_id=1,
        )
    # No path
    with pytest.raises(ValidationError):
        TaskGroupCreateStrict(
            venv_path="/b",
            version="c",
            python_version="d",
            origin=TaskGroupOriginEnum.WHEELFILE,
            archive_path="/a",
            pkg_name="x",
            user_id=1,
            resource_id=1,
        )
    # No venv_path
    with pytest.raises(ValidationError):
        TaskGroupCreateStrict(
            path="/a",
            version="c",
            python_version="d",
            origin=TaskGroupOriginEnum.WHEELFILE,
            archive_path="/a",
            pkg_name="x",
            user_id=1,
            resource_id=1,
        )
    # No version
    with pytest.raises(ValidationError):
        TaskGroupCreateStrict(
            path="/a",
            venv_path="/b",
            python_version="d",
            origin=TaskGroupOriginEnum.WHEELFILE,
            archive_path="/a",
            pkg_name="x",
            user_id=1,
            resource_id=1,
        )
    # No python_version
    with pytest.raises(ValidationError):
        TaskGroupCreateStrict(
            path="/a",
            venv_path="/b",
            version="c",
            origin=TaskGroupOriginEnum.WHEELFILE,
            archive_path="/a",
            pkg_name="x",
            user_id=1,
            resource_id=1,
        )
