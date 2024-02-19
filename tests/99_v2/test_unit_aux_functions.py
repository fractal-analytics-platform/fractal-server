import pytest
from images import _deduplicate_list_of_dicts
from runner import _validate_parallelization_list_valid


def test_validate_parallelization_list():

    # Missing path
    PARALLELIZATION_LIST = [dict()]
    CURRENT_IMAGE_PATHS = []
    with pytest.raises(ValueError):
        _validate_parallelization_list_valid(
            parallelization_list=PARALLELIZATION_LIST,
            current_image_paths=CURRENT_IMAGE_PATHS,
        )

    # Path not in current image paths
    PARALLELIZATION_LIST = [dict(path="asd")]
    CURRENT_IMAGE_PATHS = []
    with pytest.raises(ValueError):
        _validate_parallelization_list_valid(
            parallelization_list=PARALLELIZATION_LIST,
            current_image_paths=CURRENT_IMAGE_PATHS,
        )

    # Invalid buffer attributes
    PARALLELIZATION_LIST = [dict(path="asd", buffer={})]
    CURRENT_IMAGE_PATHS = ["asd"]
    with pytest.raises(ValueError):
        _validate_parallelization_list_valid(
            parallelization_list=PARALLELIZATION_LIST,
            current_image_paths=CURRENT_IMAGE_PATHS,
        )

    # Invalid `buffer`` attribute
    PARALLELIZATION_LIST = [dict(path="asd", buffer={})]
    CURRENT_IMAGE_PATHS = ["asd"]
    with pytest.raises(ValueError):
        _validate_parallelization_list_valid(
            parallelization_list=PARALLELIZATION_LIST,
            current_image_paths=CURRENT_IMAGE_PATHS,
        )

    # Invalid `root_dir` attribute
    PARALLELIZATION_LIST = [dict(path="asd", root_dir="/something")]
    CURRENT_IMAGE_PATHS = ["asd"]
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
    new = _deduplicate_list_of_dicts(old)
    assert len(new) == 2
    old = [dict(a=1), dict(a=1), dict(b=2), dict(a=1)]
    new = _deduplicate_list_of_dicts(old)
    assert len(new) == 2
