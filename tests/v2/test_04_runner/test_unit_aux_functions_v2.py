import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.images import SingleImageTaskOutput
from fractal_server.runner.exceptions import TaskOutputValidationError
from fractal_server.runner.executors.call_command_wrapper import MAX_LEN_STDERR
from fractal_server.runner.executors.call_command_wrapper import (
    placeholder_if_too_long,
)
from fractal_server.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.runner.v2.merge_outputs import merge_outputs
from fractal_server.runner.v2.runner_functions import _process_init_task_output
from fractal_server.runner.v2.runner_functions import _process_task_output
from fractal_server.runner.v2.task_interface import InitArgsModel
from fractal_server.runner.v2.task_interface import InitTaskOutput
from fractal_server.runner.v2.task_interface import TaskOutput
from fractal_server.runner.v2.task_interface import (
    _cast_and_validate_InitTaskOutput,
)
from fractal_server.runner.v2.task_interface import (
    _cast_and_validate_TaskOutput,
)


def test_deduplicate_list_of_dicts():
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
    assert TaskOutput(image_list_removals=["/tmp//"]).image_list_removals == [
        "/tmp"
    ]
    with pytest.raises(ValidationError):
        TaskOutput(image_list_removals=["http://url.json"])


def test_cast_and_validate_functions():
    _cast_and_validate_TaskOutput(
        dict(image_list_updates=[dict(zarr_url="/some/image")])
    )

    with pytest.raises(TaskOutputValidationError):
        _cast_and_validate_TaskOutput(dict(invalid=True))

    _cast_and_validate_InitTaskOutput(
        dict(
            parallelization_list=[
                dict(zarr_url="/tmp", init_args=dict(key="value"))
            ]
        )
    )
    with pytest.raises(TaskOutputValidationError):
        _cast_and_validate_InitTaskOutput(dict(invalid=True))


def test_merge_outputs():
    # 1
    merged = merge_outputs([])
    assert merged == TaskOutput()

    # 2
    task_outputs = [
        TaskOutput(
            image_list_updates=[
                SingleImageTaskOutput(zarr_url="/a"),
                SingleImageTaskOutput(zarr_url="/b"),
            ],
            image_list_removals=["/x", "/y", "/z"],
        ),
        TaskOutput(
            image_list_updates=[
                SingleImageTaskOutput(zarr_url="/c"),
                SingleImageTaskOutput(zarr_url="/a"),
            ],
            image_list_removals=["/x", "/w", "/z"],
        ),
    ]
    merged = merge_outputs(task_outputs)
    assert merged.image_list_updates == [
        SingleImageTaskOutput(zarr_url="/a"),
        SingleImageTaskOutput(zarr_url="/b"),
        SingleImageTaskOutput(zarr_url="/c"),
    ]
    assert set(merged.image_list_removals) == {"/x", "/y", "/z", "/w"}


def test_process_task_output():
    oe = _process_task_output(result=None, exception=None)
    assert oe.task_output == TaskOutput()
    assert oe.exception is None

    REMOVALS = ["/a", "/b", "/c"]
    oe = _process_task_output(
        result={"image_list_removals": REMOVALS},
        exception=None,
    )
    assert oe.task_output == TaskOutput(image_list_removals=REMOVALS)
    assert oe.exception is None

    oe = _process_task_output(result={"invalid": "dict"}, exception=None)
    assert isinstance(oe.exception, TaskOutputValidationError)
    assert oe.task_output is None

    EXCEPTION = RuntimeError("error message")
    oe = _process_task_output(result=None, exception=EXCEPTION)
    assert oe.task_output is None
    assert oe.exception == EXCEPTION


def test_process_init_task_output():
    oe = _process_init_task_output(result=None, exception=None)
    assert oe.task_output == InitTaskOutput()
    assert oe.exception is None

    PARALLELIZATION_LIST = [
        dict(zarr_url="/a1", init_args=dict(arg=1)),
        dict(zarr_url="/a2", init_args=dict(arg=2)),
    ]
    oe = _process_init_task_output(
        result={"parallelization_list": PARALLELIZATION_LIST},
        exception=None,
    )
    assert oe.task_output == InitTaskOutput(
        parallelization_list=PARALLELIZATION_LIST
    )
    assert oe.exception is None

    oe = _process_init_task_output(result={"invalid": "dict"}, exception=None)
    assert isinstance(oe.exception, TaskOutputValidationError)
    assert oe.task_output is None

    EXCEPTION = RuntimeError("error message")
    oe = _process_init_task_output(result=None, exception=EXCEPTION)
    assert oe.task_output is None
    assert oe.exception == EXCEPTION


def test_placeholder_if_too_long():
    string = "x" * MAX_LEN_STDERR
    assert string == placeholder_if_too_long(string)

    string = "x" * (MAX_LEN_STDERR + 1)
    assert string != placeholder_if_too_long(string)
    assert str(MAX_LEN_STDERR + 1) in placeholder_if_too_long(string)
