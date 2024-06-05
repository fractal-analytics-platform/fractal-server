from pathlib import Path

import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.runner_functions import (
    _cast_and_validate_InitTaskOutput,
)
from fractal_server.app.runner.v2.runner_functions import (
    _cast_and_validate_TaskOutput,
)
from fractal_server.app.runner.v2.task_interface import InitArgsModel
from fractal_server.app.runner.v2.task_interface import TaskOutput
from fractal_server.app.runner.v2.v1_compat import convert_v2_args_into_v1


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


def test_convert_v2_args_into_v1(tmp_path: Path):
    kwargs_v2 = dict(
        zarr_url=(tmp_path / "input_path/plate.zarr/B/03/0").as_posix(),
        something="else",
        metadata="this will be overwritten",
        component="this will be overwritten",
    )

    # Image
    kwargs_v1 = convert_v2_args_into_v1(
        kwargs_v2, parallelization_level="image"
    )
    PATH = (tmp_path / "input_path").as_posix()
    assert kwargs_v1 == {
        "input_paths": [PATH],
        "output_path": PATH,
        "metadata": {},
        "component": "plate.zarr/B/03/0",
        "something": "else",
    }

    # Well
    kwargs_v1 = convert_v2_args_into_v1(
        kwargs_v2, parallelization_level="well"
    )
    PATH = (tmp_path / "input_path").as_posix()
    assert kwargs_v1 == {
        "input_paths": [PATH],
        "output_path": PATH,
        "metadata": {},
        "component": "plate.zarr/B/03",
        "something": "else",
    }

    # Plate
    kwargs_v1 = convert_v2_args_into_v1(
        kwargs_v2, parallelization_level="plate"
    )
    PATH = (tmp_path / "input_path").as_posix()
    assert kwargs_v1 == {
        "input_paths": [PATH],
        "output_path": PATH,
        "metadata": {},
        "component": "plate.zarr",
        "something": "else",
    }

    # None
    with pytest.raises(ValueError):
        convert_v2_args_into_v1(kwargs_v2, parallelization_level=None)


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
