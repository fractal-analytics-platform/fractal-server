import pytest

from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.executors.base_runner import BaseRunner


def test_NotImplementedError_methods():
    runner = BaseRunner()
    with pytest.raises(NotImplementedError):
        runner.shutdown()
    with pytest.raises(NotImplementedError):
        runner.submit(
            func=None,
            parameters=None,
            history_run_id=None,
            task_type=None,
        )
    with pytest.raises(NotImplementedError):
        runner.multisubmit(
            func=None,
            list_parameters=None,
            history_run_id=None,
            task_type=None,
        )


def test_validate_submit_parameters():
    runner = BaseRunner()
    validate_submit_parameters = runner.validate_submit_parameters
    with pytest.raises(
        ValueError,
        match="must be a dictionary",
    ):
        validate_submit_parameters(
            parameters=None,
            task_type="non_parallel",
        )

    with pytest.raises(
        ValueError,
        match="Invalid task_type",
    ):
        validate_submit_parameters(
            parameters={
                "zarr_urls": [],
                "arg1": "value1",
                _COMPONENT_KEY_: "xxx",
            },
            task_type="parallel",
        )

    validate_submit_parameters(
        {
            "zarr_urls": [],
            "arg1": "value1",
            _COMPONENT_KEY_: "xxx",
        },
        task_type="non_parallel",
    )
    validate_submit_parameters(
        {
            "zarr_urls": [],
            "arg1": "value1",
            _COMPONENT_KEY_: "xxx",
        },
        task_type="compound",
    )
    validate_submit_parameters(
        {
            "arg1": "value1",
            _COMPONENT_KEY_: "xxx",
        },
        task_type="converter_non_parallel",
    )

    with pytest.raises(ValueError, match="No 'zarr_urls'"):
        validate_submit_parameters(
            {
                "arg1": "value1",
                _COMPONENT_KEY_: "xxx",
            },
            task_type="non_parallel",
        )
    with pytest.raises(ValueError, match="No 'zarr_urls'"):
        validate_submit_parameters(
            {
                "arg1": "value1",
                _COMPONENT_KEY_: "xxx",
            },
            task_type="compound",
        )
    with pytest.raises(ValueError, match="Forbidden 'zarr_urls'"):
        validate_submit_parameters(
            {
                "zarr_urls": [],
                "arg1": "value1",
                _COMPONENT_KEY_: "xxx",
            },
            task_type="converter_non_parallel",
        )

    with pytest.raises(ValueError, match=f"No '{_COMPONENT_KEY_}'"):
        validate_submit_parameters(
            {
                "zarr_urls": [],
                "arg1": "value1",
            },
            task_type="non_parallel",
        )


def test_validate_multisubmit_parameters():
    runner = BaseRunner()
    validate_multisubmit_parameters = runner.validate_multisubmit_parameters
    with pytest.raises(
        ValueError,
        match="must be a list",
    ):
        validate_multisubmit_parameters(
            list_parameters=None,
            task_type="parallel",
        )

    with pytest.raises(
        ValueError,
        match="must be a dictionary",
    ):
        validate_multisubmit_parameters(
            list_parameters=[None],
            task_type="parallel",
        )

    validate_multisubmit_parameters(
        list_parameters=[
            {
                "zarr_url": "/some",
                "arg1": "value1",
                _COMPONENT_KEY_: "xxx",
            }
        ],
        task_type="parallel",
    )

    with pytest.raises(ValueError, match="No 'zarr_url'"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "arg1": "value1",
                    _COMPONENT_KEY_: "xxx",
                }
            ],
            task_type="parallel",
        )

    with pytest.raises(ValueError, match="Invalid task_type"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/something",
                    "arg1": "value1",
                    _COMPONENT_KEY_: "xxx",
                }
            ],
            task_type="non_parallel",
        )

    validate_multisubmit_parameters(
        list_parameters=[
            {
                "zarr_url": "/something",
                "arg": "A",
                _COMPONENT_KEY_: "xxx",
            },
            {
                "zarr_url": "/something",
                "arg": "B",
                _COMPONENT_KEY_: "xxx",
            },
        ],
        task_type="compound",
    )

    with pytest.raises(ValueError, match="Non-unique zarr_urls"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/something",
                    "arg": "A",
                    _COMPONENT_KEY_: "xxx",
                },
                {
                    "zarr_url": "/something",
                    "arg": "B",
                    _COMPONENT_KEY_: "xxx",
                },
            ],
            task_type="parallel",
        )
