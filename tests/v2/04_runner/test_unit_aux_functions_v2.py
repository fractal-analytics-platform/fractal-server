import pytest
from devtools import debug

from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
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


def test_check_zarr_urls_are_unique():
    t = TaskOutput(image_list_updates=[dict(zarr_url="a"), dict(zarr_url="b")])
    t.check_zarr_urls_are_unique()

    t = TaskOutput(image_list_updates=[dict(zarr_url="a"), dict(zarr_url="a")])
    with pytest.raises(ValueError) as e:
        t.check_zarr_urls_are_unique()
    debug(str(e.value))

    t = TaskOutput(
        image_list_updates=[dict(zarr_url="a"), dict(zarr_url="b")],
        image_list_removals=["a"],
    )
    with pytest.raises(ValueError) as e:
        t.check_zarr_urls_are_unique()
    debug(str(e.value))
