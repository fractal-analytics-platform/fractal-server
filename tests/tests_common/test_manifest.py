import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError
from schemas import ManifestV1
from schemas import TaskManifestV1


def test_ManifestV1():
    task_without_args_schema = TaskManifestV1(
        name="Task A",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
    )
    task_with_args_schema = TaskManifestV1(
        name="Task B",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
        args_schema={"something": "else"},
    )
    task_with_docs_right_link = TaskManifestV1(
        name="Task B",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
        args_schema={"something": "else"},
        docs_link="http://www.example.org",
    )

    m = ManifestV1(
        manifest_version="1",
        task_list=[task_without_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=False,
        task_list=[task_without_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=True,
        task_list=[task_with_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=True,
        task_list=[task_with_docs_right_link],
    )
    debug(m)

    with pytest.raises(ValidationError) as e:
        TaskManifestV1(
            name="Task B",
            executable="executable",
            input_type="input_type",
            output_type="output_type",
            default_args={"arg": "val"},
            args_schema={"something": "else"},
            docs_link="htp://www.example.org",
        )
    debug(e.value)

    with pytest.raises(ValidationError) as e:
        m = ManifestV1(
            manifest_version="1",
            task_list=[task_without_args_schema, task_with_args_schema],
            has_args_schemas=True,
        )
    debug(e.value)

    with pytest.raises(ValidationError):
        ManifestV1(manifest_version="2", task_list=[task_with_args_schema])
