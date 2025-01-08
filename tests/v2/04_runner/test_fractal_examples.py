import logging
import os
from concurrent.futures import Executor
from pathlib import Path
from typing import Any

import pytest
from devtools import debug
from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import TaskV2Mock
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_zarr_url


def execute_tasks_v2(wf_task_list, workflow_dir_local, **kwargs):
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

    out = raw_execute_tasks_v2(
        wf_task_list=wf_task_list,
        workflow_dir_local=workflow_dir_local,
        **kwargs,
    )
    return out


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image["zarr_url"]) / "data").exists()


def _task_names_from_history(history: list[dict[str, Any]]) -> list[str]:
    return [item["workflowtask"]["task"]["name"] for item in history]


def image_data_exist_on_disk(image_list: list[SingleImage]):
    """
    Given an image list, check whether mock data were written to disk.
    """
    prefix = "[image_data_exist_on_disk]"
    all_images_have_data = True
    for image in image_list:
        if (Path(image["zarr_url"]) / "data").exists():
            print(f"{prefix} {image['zarr_url']} contains data")
        else:
            print(f"{prefix} {image['zarr_url']} does *not* contain data")
            all_images_have_data = False
    return all_images_have_data


async def test_fractal_demos_01(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    """
    Mock of fractal-demos/examples/01.
    """

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
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["create_ome_zarr_compound"],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    args_parallel={},
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "create_ome_zarr_compound"
        ]
        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {}
        _assert_image_data_exist(dataset_attrs["images"])
        assert len(dataset_attrs["images"]) == 2
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["illumination_correction"],
                    task_id=fractal_tasks_mock_no_db[
                        "illumination_correction"
                    ].id,
                    args_parallel=dict(overwrite_input=True),
                    id=1,
                    order=1,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )
        assert _task_names_from_history(dataset_attrs["history"]) == [
            "illumination_correction",
        ]
        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {
            "illumination_correction": True,
        }
        assert set(img["zarr_url"] for img in dataset_attrs["images"]) == {
            f"{zarr_dir}/my_plate.zarr/A/01/0",
            f"{zarr_dir}/my_plate.zarr/A/02/0",
        }

        img = find_image_by_zarr_url(
            zarr_url=f"{zarr_dir}/my_plate.zarr/A/01/0",
            images=dataset_attrs["images"],
        )["image"]
        assert img == {
            "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "attributes": {
                "well": "A01",
                "plate": "my_plate.zarr",
            },
            "types": {
                "illumination_correction": True,
                "3D": True,
            },
            "origin": None,
        }

        _assert_image_data_exist(dataset_attrs["images"])
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["MIP_compound"],
                    task_id=fractal_tasks_mock_no_db["MIP_compound"].id,
                    args_non_parallel=dict(suffix="mip"),
                    args_parallel={},
                    id=2,
                    order=2,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )
        debug(dataset_attrs)

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "MIP_compound",
        ]

        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {
            "illumination_correction": True,
            "3D": False,
        }
        img = find_image_by_zarr_url(
            zarr_url=f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
            images=dataset_attrs["images"],
        )["image"]
        assert img == {
            "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
            "origin": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "attributes": {
                "well": "A01",
                "plate": "my_plate_mip.zarr",
            },
            "types": {
                "3D": False,
                "illumination_correction": True,
            },
        }
        _assert_image_data_exist(dataset_attrs["images"])
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["cellpose_segmentation"],
                    task_id=fractal_tasks_mock_no_db[
                        "cellpose_segmentation"
                    ].id,
                    args_parallel={},
                    id=3,
                    order=3,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        debug(dataset_attrs)

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "cellpose_segmentation",
        ]


async def test_fractal_demos_01_no_overwrite(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    """
    Similar to fractal-demos/examples/01, but illumination
    correction task does not override its input images.
    """
    # The first block (up to yokogawa-to-zarr included) is identical to
    # the previous test
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["create_ome_zarr_compound"],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    id=0,
                    order=0,
                )
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )
        assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
            f"{zarr_dir}/my_plate.zarr/A/01/0",
            f"{zarr_dir}/my_plate.zarr/A/02/0",
        ]

        _assert_image_data_exist(dataset_attrs["images"])
        # Run illumination correction with overwrite_input=False
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["illumination_correction"],
                    task_id=fractal_tasks_mock_no_db[
                        "illumination_correction"
                    ].id,
                    args_parallel=dict(overwrite_input=False),
                    id=1,
                    order=1,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "illumination_correction",
        ]
        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {
            "illumination_correction": True,
        }
        assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
            f"{zarr_dir}/my_plate.zarr/A/01/0",
            f"{zarr_dir}/my_plate.zarr/A/02/0",
            f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
            f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        ]
        assert dataset_attrs["images"][0] == {
            "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "origin": None,
            "attributes": {
                "well": "A01",
                "plate": "my_plate.zarr",
            },
            "types": {
                "3D": True,
            },
        }
        assert dataset_attrs["images"][1] == {
            "zarr_url": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "origin": None,
            "attributes": {
                "well": "A02",
                "plate": "my_plate.zarr",
            },
            "types": {
                "3D": True,
            },
        }
        assert dataset_attrs["images"][2] == {
            "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
            "origin": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "attributes": {
                "well": "A01",
                "plate": "my_plate.zarr",
            },
            "types": {
                "illumination_correction": True,
                "3D": True,
            },
        }
        assert dataset_attrs["images"][3] == {
            "zarr_url": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
            "origin": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "attributes": {
                "well": "A02",
                "plate": "my_plate.zarr",
            },
            "types": {
                "3D": True,
                "illumination_correction": True,
            },
        }
        _assert_image_data_exist(dataset_attrs["images"])
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["MIP_compound"],
                    task_id=fractal_tasks_mock_no_db["MIP_compound"].id,
                    args_non_parallel=dict(suffix="mip"),
                    id=2,
                    order=2,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "MIP_compound",
        ]
        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {
            "3D": False,
            "illumination_correction": True,
        }
        assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
            f"{zarr_dir}/my_plate.zarr/A/01/0",
            f"{zarr_dir}/my_plate.zarr/A/02/0",
            f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
            f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
            f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
            f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
        ]

        assert dataset_attrs["images"][4] == {
            "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
            "origin": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
            "attributes": {
                "well": "A01",
                "plate": "my_plate_mip.zarr",
            },
            "types": {
                "3D": False,
                "illumination_correction": True,
            },
        }
        assert dataset_attrs["images"][5] == {
            "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
            "origin": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
            "attributes": {
                "well": "A02",
                "plate": "my_plate_mip.zarr",
            },
            "types": {
                "3D": False,
                "illumination_correction": True,
            },
        }

        assert dataset_attrs["filters"]["attributes"] == {}
        assert dataset_attrs["filters"]["types"] == {
            "3D": False,
            "illumination_correction": True,
        }

        _assert_image_data_exist(dataset_attrs["images"])
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["cellpose_segmentation"],
                    task_id=fractal_tasks_mock_no_db[
                        "cellpose_segmentation"
                    ].id,
                    id=3,
                    order=3,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        assert _task_names_from_history(dataset_attrs["history"]) == [
            "cellpose_segmentation",
        ]


async def test_registration_no_overwrite(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    """
    Test registration workflow, based on four tasks.
    """

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
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "create_ome_zarr_multiplex_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_multiplex_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    id=0,
                    order=0,
                ),
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )
        # Run init registration
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "calculate_registration_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "calculate_registration_compound"
                    ].id,
                    args_non_parallel={"ref_acquisition": 0},
                    id=1,
                    order=1,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # In all non-reference-cycle images, a certain table was updated
        for image in dataset_attrs["images"]:
            if image["attributes"]["acquisition"] == 0:
                assert not os.path.isfile(
                    f"{image['zarr_url']}/registration_table"
                )
            else:
                assert os.path.isfile(
                    f"{image['zarr_url']}/registration_table"
                )

        # Run find_registration_consensus
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "find_registration_consensus"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "find_registration_consensus"
                    ].id,
                    id=2,
                    order=2,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # In all images, a certain (post-consensus) table was updated
        for image in dataset_attrs["images"]:
            assert os.path.isfile(
                f"{image['zarr_url']}/registration_table_final"
            )

        # The image list still has the original six images only
        assert len(dataset_attrs["images"]) == 6

        # Run apply_registration_to_image
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "apply_registration_to_image"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "apply_registration_to_image"
                    ].id,
                    args_parallel={"overwrite_input": False},
                    id=3,
                    order=3,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # A new copy of each image was created
        assert len(dataset_attrs["images"]) == 12


async def test_registration_overwrite(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    """
    Test registration workflow, based on four tasks.
    """

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
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "create_ome_zarr_multiplex_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_multiplex_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    id=0,
                    order=0,
                ),
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )

        # Run init registration
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "calculate_registration_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "calculate_registration_compound"
                    ].id,
                    args_non_parallel={"ref_acquisition": 0},
                    order=1,
                    id=1,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # In all non-reference-cycle images, a certain table was updated
        for image in dataset_attrs["images"]:
            if image["attributes"]["acquisition"] == 0:
                assert not os.path.isfile(
                    f"{image['zarr_url']}/registration_table"
                )
            else:
                assert os.path.isfile(
                    f"{image['zarr_url']}/registration_table"
                )

        # Run find_registration_consensus
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "find_registration_consensus"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "find_registration_consensus"
                    ].id,
                    id=2,
                    order=2,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # In all images, a certain (post-consensus) table was updated
        for image in dataset_attrs["images"]:
            assert os.path.isfile(
                f"{image['zarr_url']}/registration_table_final"
            )

        # The image list still has the original six images only
        assert len(dataset_attrs["images"]) == 6

        # Run apply_registration_to_image
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "apply_registration_to_image"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "apply_registration_to_image"
                    ].id,
                    args_parallel={"overwrite_input": True},
                    id=3,
                    order=3,
                )
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # Images are still the same number, but they are marked as registered
        assert len(dataset_attrs["images"]) == 6
        for image in dataset_attrs["images"]:
            assert image["types"]["registration"] is True


async def test_channel_parallelization_with_overwrite(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        # Run create_ome_zarr+yokogawa_to_zarr
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["create_ome_zarr_compound"],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    id=0,
                    order=0,
                ),
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )

        # Run illumination_correction_compound
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "illumination_correction_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "illumination_correction_compound"
                    ].id,
                    args_non_parallel=dict(overwrite_input=True),
                    args_parallel=dict(another_argument="something"),
                    id=1,
                    order=1,
                ),
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # Check that there are now 2 images
        assert len(dataset_attrs["images"]) == 2


async def test_channel_parallelization_no_overwrite(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
    fractal_tasks_mock_no_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        # Run create_ome_zarr+yokogawa_to_zarr
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db["create_ome_zarr_compound"],
                    task_id=fractal_tasks_mock_no_db[
                        "create_ome_zarr_compound"
                    ].id,
                    args_non_parallel=dict(image_dir="/tmp/input_images"),
                    id=0,
                    order=0,
                ),
            ],
            dataset=dataset,
            **execute_tasks_v2_args,
        )

        # Run illumination_correction_compound
        dataset_with_attrs = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
        )
        dataset_attrs = execute_tasks_v2(
            wf_task_list=[
                WorkflowTaskV2Mock(
                    task=fractal_tasks_mock_no_db[
                        "illumination_correction_compound"
                    ],
                    task_id=fractal_tasks_mock_no_db[
                        "illumination_correction_compound"
                    ].id,
                    args_non_parallel=dict(overwrite_input=False),
                    args_parallel=dict(another_argument="something"),
                    id=1,
                    order=1,
                ),
            ],
            dataset=dataset_with_attrs,
            **execute_tasks_v2_args,
        )

        # Check that there are now 4 images
        assert len(dataset_attrs["images"]) == 4


async def test_invalid_filtered_image_list(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    tmp_path: Path,
    executor: Executor,
):
    """
    Validation of the filtered image list against task input_types fails.
    """

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=tmp_path / "job_dir",
    )

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    zarr_url = Path(zarr_dir, "my_image").as_posix()
    image = SingleImage(zarr_url=zarr_url, attributes={}, types={}).dict()
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir, images=[image]
        )
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_v2(
                wf_task_list=[
                    WorkflowTaskV2Mock(
                        task=TaskV2Mock(
                            id=0,
                            name="name",
                            source="source",
                            command_non_parallel="cmd",
                            type="non_parallel",
                            input_types=dict(invalid=True),
                        ),
                        task_id=0,
                        id=0,
                        order=0,
                    )
                ],
                dataset=dataset,
                **execute_tasks_v2_args,
            )
        assert "Invalid filtered image list" in str(e.value)
