from typing import Optional
from typing import Union

import pytest
from devtools import debug

from fractal_server.app.runner.common import set_start_and_last_task_index


cases = [
    [10, None, None, (0, 9)],
    [10, 2, None, (2, 9)],
    [10, None, 7, (0, 7)],
    [10, -1, None, "cannot be negative"],
    [10, None, -1, "cannot be negative"],
    [10, 2, 1, "cannot be larger than last"],
    [10, 1, 20, "cannot be larger than ("],
]


@pytest.mark.parametrize(
    "num_tasks,first_task_index,last_task_index,expected",
    cases,
)
def test_set_start_and_last_task_index(
    num_tasks: int,
    first_task_index: Optional[int],
    last_task_index: Optional[int],
    expected: Union[tuple[int, int], str],
):
    if isinstance(expected, tuple):
        res = set_start_and_last_task_index(
            num_tasks, first_task_index, last_task_index
        )
        assert res == expected
    elif isinstance(expected, str):
        with pytest.raises(ValueError) as e:
            res = set_start_and_last_task_index(
                num_tasks, first_task_index, last_task_index
            )
        assert expected in str(e.value)
        debug(str(e.value))
