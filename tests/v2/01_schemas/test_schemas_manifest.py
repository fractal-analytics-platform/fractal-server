import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.app.schemas.v2.manifest import TaskManifestV2


def msg(e: pytest.ExceptionInfo) -> str:
    return e.value.errors()[0]["msg"]


def test_TaskManifestV2():

    assert TaskManifestV2(name="task", executable_parallel="exec")
    assert TaskManifestV2(name="task", executable_non_parallel="exec")
    assert TaskManifestV2(
        name="task", executable_parallel="exec", executable_non_parallel="exec"
    )

    # 1: no executable
    with pytest.raises(ValidationError):
        TaskManifestV2(name="task")

    # 2: parallel with non_parallel meta
    with pytest.raises(ValidationError) as e:
        TaskManifestV2(
            name="task",
            executable_parallel="exec",
            meta_non_parallel={"a": "b"},
        )
    assert "`TaskManifestV2.meta_non_parallel` must be an empty dict" in msg(e)

    # 3: parallel with non_parallel args_schema
    with pytest.raises(ValidationError) as e:
        TaskManifestV2(
            name="task",
            executable_parallel="exec",
            args_schema_non_parallel={"a": "b"},
        )
    assert "`TaskManifestV2.args_schema_non_parallel` must be None" in msg(e)

    # 4: non_parallel with parallel meta
    with pytest.raises(ValidationError) as e:
        TaskManifestV2(
            name="task",
            executable_non_parallel="exec",
            meta_parallel={"a": "b"},
        )
    assert "`TaskManifestV2.meta_parallel` must be an empty dict" in msg(e)

    # 5: non_parallel with parallel args_schema
    with pytest.raises(ValidationError) as e:
        TaskManifestV2(
            name="task",
            executable_non_parallel="exec",
            args_schema_parallel={"a": "b"},
        )
    assert "`TaskManifestV2.args_schema_parallel` must be None" in msg(e)


def test_ManifestV2():
    assert ManifestV2(manifest_version=2, task_list=[])
    assert ManifestV2(manifest_version="2", task_list=[])

    compound_both_schemas = TaskManifestV2(
        name="task",
        executable_parallel="exec",
        args_schema_parallel={"a": "b"},
        executable_non_parallel="exec",
        args_schema_non_parallel={"a": "b"},
    )
    compound_just_parallel_schemas = TaskManifestV2(
        name="task",
        executable_parallel="exec",
        args_schema_parallel={"a": "b"},
        executable_non_parallel="exec",
    )
    compound_just_non_parallel_schemas = TaskManifestV2(
        name="task",
        executable_parallel="exec",
        executable_non_parallel="exec",
        args_schema_non_parallel={"a": "b"},
    )
    compound_no_schemas = TaskManifestV2(
        name="task", executable_parallel="exec", executable_non_parallel="exec"
    )

    parallel_schema = TaskManifestV2(
        name="task",
        executable_parallel="exec",
        args_schema_parallel={"a": "b"},
    )
    parallel_no_schema = TaskManifestV2(
        name="task", executable_parallel="exec"
    )

    non_parallel_schema = TaskManifestV2(
        name="task",
        executable_non_parallel="exec",
        args_schema_non_parallel={"a": "b"},
    )
    non_parallel_no_schema = TaskManifestV2(
        name="task", executable_non_parallel="exec"
    )

    assert ManifestV2(
        manifest_version="2",
        has_args_schemas=True,
        task_list=[
            compound_both_schemas,
            parallel_schema,
            non_parallel_schema,
        ],
    )

    # 1: invalid manifest_version
    with pytest.raises(ValidationError) as e:
        ManifestV2(manifest_version=1, task_list=[])
    assert "Wrong manifest version" in msg(e)

    # 2: compound_just_parallel_schemas
    with pytest.raises(ValidationError) as e:
        ManifestV2(
            manifest_version="2",
            has_args_schemas=True,
            task_list=[
                compound_just_parallel_schemas,
                parallel_schema,
                non_parallel_schema,
            ],
        )
    assert "Manifest has has_args_schemas=True" in msg(e)

    # 2: compound_just_parallel_schemas
    with pytest.raises(ValidationError) as e:
        ManifestV2(
            manifest_version="2",
            has_args_schemas=True,
            task_list=[
                compound_just_non_parallel_schemas,
                parallel_schema,
                non_parallel_schema,
            ],
        )
    assert "Manifest has has_args_schemas=True" in msg(e)

    # 3: compound_no_schemas
    with pytest.raises(ValidationError) as e:
        ManifestV2(
            manifest_version="2",
            has_args_schemas=True,
            task_list=[
                compound_no_schemas,
                parallel_schema,
                non_parallel_schema,
            ],
        )
    assert "Manifest has has_args_schemas=True" in msg(e)

    # 4: parallel_no_schema
    with pytest.raises(ValidationError) as e:
        ManifestV2(
            manifest_version="2",
            has_args_schemas=True,
            task_list=[
                compound_both_schemas,
                parallel_no_schema,
                non_parallel_schema,
            ],
        )
    assert "Manifest has has_args_schemas=True" in msg(e)

    # 4: non_parallel_no_schema
    with pytest.raises(ValidationError) as e:
        ManifestV2(
            manifest_version="2",
            has_args_schemas=True,
            task_list=[
                compound_both_schemas,
                parallel_schema,
                non_parallel_no_schema,
            ],
        )
    assert "Manifest has has_args_schemas=True" in msg(e)
