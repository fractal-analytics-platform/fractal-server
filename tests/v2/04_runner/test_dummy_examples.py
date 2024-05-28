import logging
from concurrent.futures import Executor
from copy import deepcopy
from pathlib import Path

import pytest
from devtools import debug
from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import DatasetV2Mock
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.urls import normalize_url


def execute_tasks_v2(wf_task_list, workflow_dir, **kwargs):
    from fractal_server.app.runner.task_files import task_subfolder_name
    from fractal_server.app.runner.v2.runner import (
        execute_tasks_v2 as raw_execute_tasks_v2,
    )

    for wftask in wf_task_list:
        subfolder = workflow_dir / task_subfolder_name(
            order=wftask.order, task_name=wftask.task.name
        )
        logging.info(f"Now creating {subfolder.as_posix()}")
        subfolder.mkdir(parents=True)

    out = raw_execute_tasks_v2(
        wf_task_list=wf_task_list, workflow_dir=workflow_dir, **kwargs
    )
    return out


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
    IMAGES = [dict(zarr_url=Path(zarr_dir, "my-image").as_posix())]
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                    args_non_parallel={
                        "full_new_image": dict(
                            zarr_url=IMAGES[0]["zarr_url"], origin="/somewhere"
                        )
                    },
                    id=2,
                    order=2,
                )
            ],
            dataset=DatasetV2Mock(
                name="dataset", zarr_dir=zarr_dir, images=IMAGES
            ),
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert (
        "Cannot edit an image with zarr_url different from origin."
        in error_msg
    )

    # Fail because types filters are set twice
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir_2",
        workflow_dir_user=tmp_path / "job_dir_2",
    )
    PATCHED_TASK = deepcopy(
        fractal_tasks_mock_venv["dummy_insert_single_image"]
    )
    KEY = "something"
    PATCHED_TASK.output_types = {KEY: True}
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=PATCHED_TASK,
                    args_non_parallel={"types": {KEY: True}},
                    id=2,
                    order=2,
                )
            ],
            dataset=DatasetV2Mock(
                name="dataset", zarr_dir=zarr_dir, images=IMAGES
            ),
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "Some type filters are being set twice" in error_msg

    # Fail because new image is not relative to zarr_dir
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir_3",
        workflow_dir_user=tmp_path / "job_dir_3",
    )
    with pytest.raises(JobExecutionError) as e:
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

    # Fail because new image's zarr_url is equal to zarr_dir
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_venv["dummy_insert_single_image"],
                    args_non_parallel={"fail_2": True},
                    id=3,
                    order=3,
                )
            ],
            dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "Cannot create image if zarr_url is equal to zarr_dir" in error_msg


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
    with pytest.raises(JobExecutionError) as e:
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


def test_default_inclusion_of_images(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
    """
    Ref
    https://github.com/fractal-analytics-platform/fractal-server/issues/1374
    """
    # Prepare dataset
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    images = [
        dict(
            zarr_url=Path(zarr_dir, "my_image").as_posix(),
            attributes={},
            types={},
        )
    ]
    dataset_pre = DatasetV2Mock(
        name="dataset", zarr_dir=zarr_dir, images=images
    )

    # Run
    dataset_attrs = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["generic_task_parallel"],
                id=0,
                order=0,
            )
        ],
        dataset=dataset_pre,
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    image = dataset_attrs["images"][0]
    debug(dataset_attrs)
    debug(image)
    assert image["types"] == dict(my_type=True)
