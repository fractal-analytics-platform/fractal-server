import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.merge_outputs import merge_outputs
from fractal_server.app.runner.v2.runner_functions import (
    _cast_and_validate_InitTaskOutput,
)
from fractal_server.app.runner.v2.runner_functions import (
    _cast_and_validate_TaskOutput,
)
from fractal_server.app.runner.v2.task_interface import InitArgsModel
from fractal_server.app.runner.v2.task_interface import TaskOutput


def test_deduplicate_list_of_dicts():
    #
    old = [
        InitArgsModel(zarr_url="/asd", init_args=dict(a=1)),
        InitArgsModel(zarr_url="/asd", init_args=dict(a=2)),
    ]
    new = deduplicate_list(old)
    assert len(new) == 2

    #
    old = [
        InitArgsModel(zarr_url="/asd", init_args=dict(a=1)),
        InitArgsModel(zarr_url="/asd", init_args=dict(a=1)),
        InitArgsModel(zarr_url="/asd", init_args=dict(a=2)),
    ]
    debug(old)
    new = deduplicate_list(old)
    debug(new)
    assert len(new) == 2


def test_task_output():
    # Test 'check_zarr_urls_are_unique'
    t = TaskOutput(
        image_list_updates=[dict(zarr_url="/a"), dict(zarr_url="/b")]
    )
    t.check_zarr_urls_are_unique()

    t = TaskOutput(
        image_list_updates=[dict(zarr_url="/a"), dict(zarr_url="/a")]
    )
    with pytest.raises(ValueError) as e:
        t.check_zarr_urls_are_unique()
    debug(str(e.value))

    t = TaskOutput(
        image_list_updates=[dict(zarr_url="/a"), dict(zarr_url="/b")],
        image_list_removals=["/a"],
    )
    with pytest.raises(ValueError) as e:
        t.check_zarr_urls_are_unique()
    debug(str(e.value))
    # Test 'normalize_paths'
    assert TaskOutput(
        image_list_removals=["/a/b/../c/", "/tmp//"]
    ).image_list_removals == ["/a/c", "/tmp"]
    with pytest.raises(ValidationError):
        TaskOutput(image_list_removals=["s3://BUCKET"])
    with pytest.raises(ValidationError):
        TaskOutput(image_list_removals=["http://url.json"])


def test_cast_and_validate_functions():

    _cast_and_validate_TaskOutput(
        dict(filters={}, image_list_updates=[dict(zarr_url="/some/image")])
    )

    with pytest.raises(JobExecutionError):
        _cast_and_validate_TaskOutput(dict(invalid=True))

    _cast_and_validate_InitTaskOutput(
        dict(
            parallelization_list=[
                dict(zarr_url="/tmp", init_args=dict(key="value"))
            ]
        )
    )
    with pytest.raises(JobExecutionError):
        _cast_and_validate_InitTaskOutput(dict(invalid=True))


def test_merge_outputs():

    # 1
    task_outputs = [
        TaskOutput(type_filters={"a": True}),
        TaskOutput(type_filters={"a": True}),
    ]
    merged = merge_outputs(task_outputs)
    assert merged.type_filters == {"a": True}

    # 2
    task_outputs = [
        TaskOutput(type_filters={"a": True}),
        TaskOutput(type_filters={"b": True}),
    ]
    with pytest.raises(ValueError):
        merge_outputs(task_outputs)

    # 3
    task_outputs = [
        TaskOutput(type_filters={"a": True}),
        TaskOutput(type_filters={"a": False}),
    ]
    with pytest.raises(ValueError):
        merge_outputs(task_outputs)

    # 4
    merged = merge_outputs([])
    assert merged == TaskOutput()
