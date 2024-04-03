from devtools import debug

from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.task_interface import InitArgsModel


def test_deduplicate_list_of_dicts():
    #
    old = [
        InitArgsModel(path="/asd", init_args=dict(a=1)),
        InitArgsModel(path="/asd", init_args=dict(a=2)),
    ]
    new = deduplicate_list(old, PydanticModel=InitArgsModel)
    assert len(new) == 2

    #
    old = [
        InitArgsModel(path="/asd", init_args=dict(a=1)),
        InitArgsModel(path="/asd", init_args=dict(a=1)),
        InitArgsModel(path="/asd", init_args=dict(a=2)),
    ]
    debug(old)
    new = deduplicate_list(old, PydanticModel=InitArgsModel)
    debug(new)
    assert len(new) == 2
