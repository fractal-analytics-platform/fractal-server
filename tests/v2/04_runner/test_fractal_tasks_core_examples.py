import json
import os
import sys
from concurrent.futures import Executor
from pathlib import Path

import pytest
from devtools import debug
from v2_mock_models import DatasetV2Mock
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.executors.local import FractalThreadPoolExecutor
from fractal_server.app.runner.v2 import execute_tasks_v2
from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_path


@pytest.fixture()
def executor():
    with FractalThreadPoolExecutor() as e:
        yield e


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image.path) / "data").exists()


def image_data_exist_on_disk(image_list: list[SingleImage]):
    """
    Given an image list, check whether mock data were written to disk.
    """
    prefix = "[image_data_exist_on_disk]"
    all_images_have_data = True
    for image in image_list:
        if (Path(image.path) / "data").exists():
            print(f"{prefix} {image.path} contains data")
        else:
            print(f"{prefix} {image.path} does *not* contain data")
            all_images_have_data = False
    return all_images_have_data


@pytest.fixture
def fractal_tasks_mock_task_list(testdata_path) -> dict:
    from v2_mock_models import TaskV2Mock

    src_dir = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/src"
        / "fractal_tasks_mock/"
    )

    python_str = sys.executable
    with (src_dir / "__FRACTAL_MANIFEST__.json").open("r") as f:
        manifest = json.load(f)

    task_dict = {}
    for task in manifest["task_list"]:
        task_attributes = dict(
            name=task["name"],
            source=task["name"].replace(" ", "_"),
        )
        if task["name"] == "MIP_compound":
            task_attributes.update(
                dict(
                    input_types={"3D": True},
                    output_types={"3D": False},
                )
            )
        elif task["name"] in [
            "illumination_correction",
            "illumination_correction_compound",
        ]:
            task_attributes.update(
                dict(
                    input_types={"illumination_correction": False},
                    output_types={"illumination_correction": True},
                )
            )
        elif task["name"] == "apply_registration_to_image":
            task_attributes.update(
                dict(
                    input_types={"registration": False},
                    output_types={"registration": True},
                )
            )
        for step in ["non_parallel", "parallel"]:
            key = f"executable_{step}"
            if key in task.keys():
                task_path = (src_dir / task[key]).as_posix()
                task_attributes[
                    f"command_{step}"
                ] = f"{python_str} {task_path}"
        t = TaskV2Mock(**task_attributes)
        task_dict[t.name] = t

    return task_dict


def test_fractal_demos_01(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
):
    """
    Mock of fractal-demos/examples/01.
    """

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path,
    )

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                args_parallel={},
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )

    assert dataset.history == ["create_ome_zarr_compound"]
    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {}
    _assert_image_data_exist(dataset.images)
    debug(dataset)
    assert len(dataset.images) == 2

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["illumination_correction"],
                args_parallel=dict(overwrite_input=True),
                id=1,
                order=1,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
    ]
    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {
        "illumination_correction": True,
    }
    assert set(dataset.image_paths) == {
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    }

    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate.zarr/A/01/0", images=dataset.images
    )["image"]
    assert img.dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
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

    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["MIP_compound"],
                args_non_parallel=dict(suffix="mip"),
                args_parallel={},
                id=2,
                order=2,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )
    debug(dataset)

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
        "MIP_compound",
    ]

    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {
        "illumination_correction": True,
        "3D": False,
    }
    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate_mip.zarr/A/01/0", images=dataset.images
    )["image"]
    assert img.dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
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
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["cellpose_segmentation"],
                args_parallel={},
                id=3,
                order=3,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    debug(dataset)

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
        "MIP_compound",
        "cellpose_segmentation",
    ]


def test_fractal_demos_01_no_overwrite(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
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
        workflow_dir=tmp_path,
    )

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    ]

    debug(dataset)
    _assert_image_data_exist(dataset.images)
    # Run illumination correction with overwrite_input=False
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["illumination_correction"],
                args_parallel=dict(overwrite_input=False),
                id=1,
                order=1,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
    ]
    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {
        "illumination_correction": True,
    }
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
    ]
    debug(dataset)

    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "origin": None,
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
        },
        "types": {
            "3D": True,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "origin": None,
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
        },
        "types": {
            "3D": True,
        },
    }
    assert dataset.images[2].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
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
    assert dataset.images[3].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
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
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["MIP_compound"],
                args_non_parallel=dict(suffix="mip"),
                id=2,
                order=2,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
        "MIP_compound",
    ]
    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {
        "3D": False,
        "illumination_correction": True,
    }
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
    ]

    assert dataset.images[4].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
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
    assert dataset.images[5].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
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

    assert dataset.filters["attributes"] == {}
    assert dataset.filters["types"] == {
        "3D": False,
        "illumination_correction": True,
    }

    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["cellpose_segmentation"],
                id=3,
                order=3,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )
    debug(dataset)

    assert dataset.history == [
        "create_ome_zarr_compound",
        "illumination_correction",
        "MIP_compound",
        "cellpose_segmentation",
    ]


def test_registration_no_overwrite(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
):
    """
    Test registration workflow, based on four tasks.
    """

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path,
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "create_ome_zarr_multiplex_compound"
                ],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                id=0,
                order=0,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )

    # Run init registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "calculate_registration_compound"
                ],
                args_non_parallel={"ref_acquisition": 0},
                id=1,
                order=1,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset.images:
        if image.attributes["acquisition"] == 0:
            assert not os.path.isfile(f"{image.path}/registration_table")
        else:
            assert os.path.isfile(f"{image.path}/registration_table")

    # Run find_registration_consensus
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "find_registration_consensus"
                ],
                id=2,
                order=2,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset.images:
        assert os.path.isfile(f"{image.path}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset.images) == 6

    # Run apply_registration_to_image
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "apply_registration_to_image"
                ],
                args_parallel={"overwrite_input": False},
                id=3,
                order=3,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # A new copy of each image was created
    assert len(dataset.images) == 12

    # Print current dataset information
    debug(dataset)


def test_registration_overwrite(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
):
    """
    Test registration workflow, based on four tasks.
    """

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path,
    )

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "create_ome_zarr_multiplex_compound"
                ],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                id=0,
                order=0,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )

    # Run init registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "calculate_registration_compound"
                ],
                args_non_parallel={"ref_acquisition": 0},
                order=1,
                id=1,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset.images:
        if image.attributes["acquisition"] == 0:
            assert not os.path.isfile(f"{image.path}/registration_table")
        else:
            assert os.path.isfile(f"{image.path}/registration_table")

    # Run find_registration_consensus
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "find_registration_consensus"
                ],
                id=2,
                order=2,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset.images:
        assert os.path.isfile(f"{image.path}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset.images) == 6

    # Run apply_registration_to_image
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "apply_registration_to_image"
                ],
                args_parallel={"overwrite_input": True},
                id=3,
                order=3,
            )
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Images are still the same number, but they are marked as registered
    assert len(dataset.images) == 6
    for image in dataset.images:
        assert image.types["registration"] is True

    # Print current dataset information
    debug(dataset)


def test_channel_parallelization_with_overwrite(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path,
    )
    # Run create_ome_zarr+yokogawa_to_zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                id=0,
                order=0,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # Run illumination_correction_compound
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "illumination_correction_compound"
                ],
                args_non_parallel=dict(overwrite_input=True),
                id=1,
                order=1,
            ),
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # Check that there are now 2 images
    assert len(dataset.images) == 2


def test_channel_parallelization_no_overwrite(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_task_list
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path,
    )
    # Run create_ome_zarr+yokogawa_to_zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                id=0,
                order=0,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # Run init_channel_parallelization
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_task_list[
                    "illumination_correction_compound"
                ],
                args_non_parallel=dict(overwrite_input=False),
                id=1,
                order=1,
            ),
        ],
        dataset=dataset,
        **execute_tasks_v2_args,
    )

    # Print current dataset information
    debug(dataset)

    # Check that there are now 4 images
    assert len(dataset.images) == 4
