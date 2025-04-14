import pytest

from ..aux_unit_runner import get_dummy_task_files
from fractal_server.app.runner.executors.base_runner import BaseRunner


def test_NotImplementedError_methods():
    runner = BaseRunner()
    with pytest.raises(NotImplementedError):
        runner.submit(
            func=None,
            parameters=None,
            history_unit_id=None,
            task_type=None,
            task_files=None,
            config=None,
        )
    with pytest.raises(NotImplementedError):
        runner.multisubmit(
            func=None,
            list_parameters=None,
            history_unit_ids=None,
            task_type=None,
            list_task_files=None,
            config=None,
            map_history_unit_id_to_index=None,
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
            },
            task_type="parallel",
        )

    validate_submit_parameters(
        {
            "zarr_urls": [],
            "arg1": "value1",
        },
        task_type="non_parallel",
    )
    validate_submit_parameters(
        {
            "zarr_urls": [],
            "arg1": "value1",
        },
        task_type="compound",
    )
    validate_submit_parameters(
        {
            "arg1": "value1",
        },
        task_type="converter_non_parallel",
    )

    with pytest.raises(ValueError, match="No 'zarr_urls'"):
        validate_submit_parameters(
            {
                "arg1": "value1",
            },
            task_type="non_parallel",
        )
    with pytest.raises(ValueError, match="No 'zarr_urls'"):
        validate_submit_parameters(
            {
                "arg1": "value1",
            },
            task_type="compound",
        )
    with pytest.raises(ValueError, match="Forbidden 'zarr_urls'"):
        validate_submit_parameters(
            {
                "zarr_urls": [],
                "arg1": "value1",
            },
            task_type="converter_non_parallel",
        )


def test_validate_multisubmit_parameters(tmp_path):
    runner = BaseRunner()
    validate_multisubmit_parameters = runner.validate_multisubmit_parameters
    with pytest.raises(
        ValueError,
        match="must be a list",
    ):
        validate_multisubmit_parameters(
            list_parameters=None,
            task_type="parallel",
            list_task_files=[get_dummy_task_files(tmp_path, component="0")],
        )

    with pytest.raises(
        ValueError,
        match="must be a dictionary",
    ):
        validate_multisubmit_parameters(
            list_parameters=[None],
            task_type="parallel",
            list_task_files=[get_dummy_task_files(tmp_path, component="0")],
        )

    validate_multisubmit_parameters(
        list_parameters=[
            {
                "zarr_url": "/some",
                "arg1": "value1",
            }
        ],
        task_type="parallel",
        list_task_files=[get_dummy_task_files(tmp_path, component="0")],
    )

    with pytest.raises(ValueError, match="No 'zarr_url'"):
        validate_multisubmit_parameters(
            list_parameters=[{"arg1": "value1"}],
            task_type="parallel",
            list_task_files=[get_dummy_task_files(tmp_path, component="0")],
        )

    with pytest.raises(ValueError, match="Invalid task_type"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/something",
                    "arg1": "value1",
                }
            ],
            task_type="non_parallel",
            list_task_files=[get_dummy_task_files(tmp_path, component="0")],
        )

    validate_multisubmit_parameters(
        list_parameters=[
            {
                "zarr_url": "/something",
                "arg": "A",
            },
            {
                "zarr_url": "/something",
                "arg": "B",
            },
        ],
        task_type="compound",
        list_task_files=[
            get_dummy_task_files(tmp_path, component="A"),
            get_dummy_task_files(tmp_path, component="B"),
        ],
    )

    with pytest.raises(ValueError, match="More than one subfolders"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/something",
                    "arg": "A",
                },
                {
                    "zarr_url": "/something",
                    "arg": "B",
                },
            ],
            task_type="compound",
            list_task_files=[
                get_dummy_task_files(tmp_path / "A", component="A"),
                get_dummy_task_files(tmp_path / "B", component="B"),
            ],
        )

    with pytest.raises(ValueError, match="Non-unique zarr_urls"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/something",
                    "arg": "A",
                },
                {
                    "zarr_url": "/something",
                    "arg": "B",
                },
            ],
            task_type="parallel",
            list_task_files=[
                get_dummy_task_files(tmp_path, component="A"),
                get_dummy_task_files(tmp_path, component="B"),
            ],
        )

    with pytest.raises(ValueError, match="differs from"):
        validate_multisubmit_parameters(
            list_parameters=[
                {
                    "zarr_url": "/A",
                    "arg": "A",
                },
                {
                    "zarr_url": "/B",
                    "arg": "B",
                },
            ],
            task_type="parallel",
            list_task_files=[
                get_dummy_task_files(tmp_path, component="A"),
            ],
        )
