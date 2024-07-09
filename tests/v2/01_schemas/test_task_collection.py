import json

import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCollectCustomV2
from fractal_server.app.schemas.v2 import TaskCollectPipV2


def test_TaskCollectPipV2():
    """
    Check that leading/trailing whitespace characters were removed
    """
    collection = TaskCollectPipV2(
        package="  package  ", package_version="  1.2.3  "
    )
    assert collection.package == "package"
    assert collection.package_version == "1.2.3"


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
            source="b",
            package_root=None,
            package_name="inject;code",
        )
    assert "Invalid package_name" in e._excinfo[1].errors()[0]["msg"]

    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="a",
            source="b",
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
            source="b",
            package_root="non_absolute_path",
            package_name=None,
        )
    assert "'package_root' must be an absolute path" in str(e.value)

    # Fail because neither 'package_root' nor 'package_name'
    with pytest.raises(ValidationError) as e:
        TaskCollectCustomV2(
            manifest=ManifestV2(**manifest_dict),
            python_interpreter="/a",
            source="source",
            package_root=None,
            package_name=None,
            version=None,
        )
    assert "One and only one must be set" in str(e.value)

    # Successful
    collection = TaskCollectCustomV2(
        manifest=ManifestV2(**manifest_dict),
        python_interpreter="  /some/python                  ",
        source="b",
        package_root=None,
        package_name="something",
    )
    # Check that trailing whitespace characters were removed
    assert collection.python_interpreter == "/some/python"
