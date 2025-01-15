import logging
from concurrent.futures import Executor
from copy import deepcopy
from pathlib import Path

import pytest
from aux_get_dataset_attrs import _get_dataset_attrs
from devtools import debug
from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.urls import normalize_url


def execute_tasks_v2(wf_task_list, workflow_dir_local, **kwargs) -> None:
    from fractal_server.app.runner.task_files import task_subfolder_name
    from fractal_server.app.runner.v2.runner import (
        execute_tasks_v2 as raw_execute_tasks_v2,
    )

    for wftask in wf_task_list:
        subfolder = workflow_dir_local / task_subfolder_name(
            order=wftask.order, task_name=wftask.task.name
        )
        logging.info(f"Now creating {subfolder.as_posix()}")
        subfolder.mkdir(parents=True)

    raw_execute_tasks_v2(
        wf_task_list=wf_task_list,
        workflow_dir_local=workflow_dir_local,
        job_attribute_filters={},
        **kwargs,
    )


async def test_dummy_insert_single_image(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        # Run successfully on an empty dataset
        debug(dataset)
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["dummy_insert_single_image"],
                    task_id=fractal_tasks_mock_no_db[
                        "dummy_insert_single_image"
                    ].id,
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )

        # Run successfully even if the image already exists
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["dummy_insert_single_image"],
                    task_id=fractal_tasks_mock_no_db[
                        "dummy_insert_single_image"
                    ].id,
                    id=1,
                    order=1,
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )
        # Fail because new image is not relative to zarr_dir
        IMAGES = [dict(zarr_url=Path(zarr_dir, "my-image").as_posix())]
        dataset_images = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, images=IMAGES
        )
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_v2(
                wf_task_list=[
                    WorkflowTaskV2Mock(
                        task=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ],
                        task_id=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ].id,
                        args_non_parallel={
                            "full_new_image": dict(
                                zarr_url=IMAGES[0]["zarr_url"],
                                origin="/somewhere",
                            )
                        },
                        id=2,
                        order=2,
                    )
                ],
                dataset=dataset_images,
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
            workflow_dir_local=tmp_path / "job_dir_2",
            workflow_dir_remote=tmp_path / "job_dir_2",
        )
        PATCHED_TASK = deepcopy(
            fractal_tasks_mock_no_db["dummy_insert_single_image"]
        )
        KEY = "something"
        PATCHED_TASK.output_types = {KEY: True}
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_v2(
                wf_task_list=[
                    WorkflowTaskV2Mock(
                        task=PATCHED_TASK,
                        task_id=PATCHED_TASK.id,
                        args_non_parallel={"types": {KEY: True}},
                        id=2,
                        order=2,
                    )
                ],
                dataset=dataset_images,
                **execute_tasks_v2_args,
            )
        error_msg = str(e.value)
        assert "Some type filters are being set twice" in error_msg

        # Fail because new image is not relative to zarr_dir
        execute_tasks_v2_args = dict(
            executor=executor,
            workflow_dir_local=tmp_path / "job_dir_3",
            workflow_dir_remote=tmp_path / "job_dir_3",
        )
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_v2(
                wf_task_list=[
                    WorkflowTaskV2Mock(
                        task=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ],
                        task_id=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ].id,
                        args_non_parallel={"fail": True},
                        id=2,
                        order=2,
                    )
                ],
                dataset=dataset,
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
                        task=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ],
                        task_id=fractal_tasks_mock_no_db[
                            "dummy_insert_single_image"
                        ].id,
                        args_non_parallel={"fail_2": True},
                        id=3,
                        order=3,
                    )
                ],
                dataset=dataset,
                **execute_tasks_v2_args,
            )
        error_msg = str(e.value)
        assert (
            "Cannot create image if zarr_url is equal to zarr_dir" in error_msg
        )


async def test_dummy_remove_images(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        # Run successfully on a dataset which includes the images to be removed
        project = await project_factory_v2(user)
        dataset_pre = await dataset_factory_v2(
            project_id=project.id,
            zarr_dir=zarr_dir,
            images=[
                dict(zarr_url=Path(zarr_dir, str(index)).as_posix())
                for index in [0, 1, 2]
            ],
        )
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["dummy_remove_images"],
                    task_id=fractal_tasks_mock_no_db["dummy_remove_images"].id,
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset_pre,
            **execute_tasks_v2_args,
        )

        # Fail when removing images that do not exist
        dataset_pre_fail = await dataset_factory_v2(
            project_id=project.id,
            zarr_dir=zarr_dir,
        )
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_v2(
                wf_task_list=[
                    WorkflowTaskV2Mock(
                        task=fractal_tasks_mock_no_db["dummy_remove_images"],
                        task_id=fractal_tasks_mock_no_db[
                            "dummy_remove_images"
                        ].id,
                        id=1,
                        order=1,
                        args_non_parallel=dict(
                            more_zarr_urls=[
                                Path(zarr_dir, "missing-image").as_posix()
                            ]
                        ),
                    )
                ],
                dataset=dataset_pre_fail,
                **execute_tasks_v2_args,
            )
        error_msg = str(e.value)
        assert "Cannot remove missing image" in error_msg


async def test_dummy_unset_attribute(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset_pre = await dataset_factory_v2(
            project_id=project.id,
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
    execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_no_db["dummy_unset_attribute"],
                task_id=fractal_tasks_mock_no_db["dummy_unset_attribute"].id,
                args_non_parallel=dict(attribute="key2"),
                id=0,
                order=0,
            )
        ],
        dataset=dataset_pre,
        **execute_tasks_v2_args,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_pre.id)
    debug(dataset_attrs["images"])
    assert "key2" not in dataset_attrs["images"][0]["attributes"].keys()

    # Unset a missing attribute (starting from dataset_pre)
    execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_no_db["dummy_unset_attribute"],
                task_id=fractal_tasks_mock_no_db["dummy_unset_attribute"].id,
                args_non_parallel=dict(attribute="missing-attribute"),
                id=1,
                order=1,
            )
        ],
        dataset=dataset_pre,
        **execute_tasks_v2_args,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_pre.id)
    assert dataset_attrs["images"][0]["attributes"] == {
        "key1": "value1",
        "key2": "value2",
    }


async def test_dummy_insert_single_image_none_attribute(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        # Run successfully on an empty dataset
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["dummy_insert_single_image"],
                    task_id=fractal_tasks_mock_no_db[
                        "dummy_insert_single_image"
                    ].id,
                    args_non_parallel=dict(
                        attributes={"attribute-name": None}
                    ),
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )
        dataset_attrs = await _get_dataset_attrs(db, dataset.id)
        debug(dataset_attrs["images"])
        assert (
            "attribute-name"
            not in dataset_attrs["images"][0]["attributes"].keys()
        )


async def test_dummy_insert_single_image_normalization(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        # Run successfully with trailing slashes
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["dummy_insert_single_image"],
                    task_id=fractal_tasks_mock_no_db[
                        "dummy_insert_single_image"
                    ].id,
                    id=0,
                    order=0,
                    args_non_parallel={"trailing_slash": True},
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )
        dataset_attrs = await _get_dataset_attrs(db, dataset.id)
        debug(dataset_attrs["images"])
        for image in dataset_attrs["images"]:
            assert normalize_url(image["zarr_url"]) == image["zarr_url"]


async def test_default_inclusion_of_images(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
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
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset_pre = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, images=images
        )

        # Run
        execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["generic_task_parallel"],
                    task_id=fractal_tasks_mock_no_db[
                        "generic_task_parallel"
                    ].id,
                    rder=0,
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset_pre,
            executor=executor,
            workflow_dir_local=tmp_path / "job_dir",
            workflow_dir_remote=tmp_path / "job_dir",
        )
        dataset_attrs = await _get_dataset_attrs(db, dataset_pre.id)
        image = dataset_attrs["images"][0]
        debug(dataset_attrs)
        debug(image)
        assert image["types"] == dict(my_type=True)
