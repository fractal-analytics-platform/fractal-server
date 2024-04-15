from concurrent.futures import Executor
from pathlib import Path

import pytest
from devtools import debug
from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import DatasetV2Mock
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.v2.runner import execute_tasks_v2
from fractal_server.urls import normalize_url


def test_dummy_insert_single_image(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully on an empty dataset
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])

    # Run successfully even if the image already exists
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                id=1,
                order=1,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])

    # Fail because new image is not relative to zarr_dir
    with pytest.raises(ValueError) as e:
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                    args_non_parallel={"fail": True},
                    id=2,
                    order=2,
                )
            ],
            dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "is not a parent directory" in error_msg
    assert zarr_dir in error_msg


def test_dummy_remove_images(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully on a dataset which includes the images to be removed
    dataset_pre = DatasetV2Mock(
        name="dataset",
        zarr_dir=zarr_dir,
        images=[
            dict(zarr_url=Path(zarr_dir, str(index)).as_posix())
            for index in [0, 1, 2]
        ],
    )
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_remove_images"],
                id=0,
                order=0,
            )
        ],
        dataset=dataset_pre,
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs)

    # Fail when removing images that do not exist
    dataset_pre = DatasetV2Mock(
        name="dataset",
        zarr_dir=zarr_dir,
    )
    with pytest.raises(ValueError) as e:
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_venv["dummy_remove_images"],
                    id=1,
                    order=1,
                    args_non_parallel=dict(
                        more_zarr_urls=[
                            Path(zarr_dir, "missing-image").as_posix()
                        ]
                    ),
                )
            ],
            dataset=dataset_pre,
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "Cannot remove missing image" in error_msg


def test_dummy_unset_attribute(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    dataset_pre = DatasetV2Mock(
        name="dataset",
        zarr_dir=zarr_dir,
        images=[
            dict(
                zarr_url=Path(zarr_dir, "my-image").as_posix(),
                attributes={"key1": "value1", "key2": "value2"},
                types={},
            )
        ],
    )

    # Unset an existing attribute (starting from dataset_pre)
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_unset_attribute"],
                args_non_parallel=dict(attribute="key2"),
                id=0,
                order=0,
            )
        ],
        dataset=dataset_pre,
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])
    assert "key2" not in dataset_attrs["images"][0]["attributes"].keys()

    # Unset a missing attribute (starting from dataset_pre)
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_unset_attribute"],
                args_non_parallel=dict(attribute="missing-attribute"),
                id=1,
                order=1,
            )
        ],
        dataset=dataset_pre,
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])
    assert dataset_attrs["images"][0]["attributes"] == {
        "key1": "value1",
        "key2": "value2",
    }


def test_dummy_insert_single_image_none_attribute(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully on an empty dataset
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                args_non_parallel=dict(attributes={"attribute-name": None}),
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])
    assert (
        "attribute-name" not in dataset_attrs["images"][0]["attributes"].keys()
    )


def test_dummy_insert_single_image_normalization(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully with trailing slashes
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                id=0,
                order=0,
                args_non_parallel={"trailing_slash": True},
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
    debug(dataset_attrs["images"])
    for image in dataset_attrs["images"]:
        assert normalize_url(image["zarr_url"]) == image["zarr_url"]
