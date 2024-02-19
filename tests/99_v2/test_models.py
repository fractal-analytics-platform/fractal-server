import pytest
from devtools import debug
from models import Task


def _dummy_function():
    pass


def test_model_task():
    NEW_FILTERS = dict(x=True, y=1, z="asd", w=None)
    task = Task(function=_dummy_function, new_filters=NEW_FILTERS)
    debug(task.new_filters)
    debug(NEW_FILTERS)
    assert task.new_filters == NEW_FILTERS

    with pytest.raises(ValueError) as e:
        Task(function=_dummy_function, new_filters=dict(key=[]))
    debug(str(e.value))

    with pytest.raises(ValueError) as e:
        Task(function=_dummy_function, new_filters=dict(key={}))
    debug(str(e.value))
