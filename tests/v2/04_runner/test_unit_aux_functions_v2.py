import pytest

from fractal_server.app.runner.v2.runner import (
    _validate_parallelization_list_valid,
)
from fractal_server.images import deduplicate_list


def test_validate_parallelization_list():

    # Missing path
    PARALLELIZATION_LIST = [dict()]
    CURRENT_IMAGE_PATHS = []
    with pytest.raises(ValueError):
        _validate_parallelization_list_valid(
            parallelization_list=PARALLELIZATION_LIST,
            current_image_paths=CURRENT_IMAGE_PATHS,
        )

    # Valid call
    PARALLELIZATION_LIST = [
        dict(path="asd", parameter=1),
        dict(path="asd", parameter=2),
        dict(path="asd", parameter=3),
    ]
    CURRENT_IMAGE_PATHS = ["asd"]
    _validate_parallelization_list_valid(
        parallelization_list=PARALLELIZATION_LIST,
        current_image_paths=CURRENT_IMAGE_PATHS,
    )


def test_deduplicate_list_of_dicts():
    old = [dict(a=1), dict(b=2)]
    new = deduplicate_list(old)
    assert len(new) == 2
    old = [dict(a=1), dict(a=1), dict(b=2), dict(a=1)]
    new = deduplicate_list(old)
    assert len(new) == 2
