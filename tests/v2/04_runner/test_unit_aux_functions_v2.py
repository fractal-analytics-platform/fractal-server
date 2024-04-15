from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
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


def test_check_zarr_urls_are_unique():
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


def test_convert_v2_args_into_v1(tmp_path: Path):
    kwargs_v2 = dict(
        zarr_url=(tmp_path / "input_path/plate.zarr/B/03/0").as_posix(),
        something="else",
        metadata="this will be overwritten",
        component="this will be overwritten",
    )
    kwargs_v1 = convert_v2_args_into_v1(kwargs_v2)
    debug(kwargs_v1)
    PATH = (tmp_path / "input_path").as_posix()
    assert kwargs_v1 == {
        "input_paths": [PATH],
        "output_path": PATH,
        "metadata": {},
        "component": "plate.zarr/B/03/0",
        "something": "else",
    }
