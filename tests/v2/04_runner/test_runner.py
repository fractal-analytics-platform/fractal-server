from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Optional

import pytest
from devtools import debug
from tasks_for_tests import dummy_task

from fractal_server.app.runner.v2.models import Dataset
from fractal_server.app.runner.v2.models import SingleImage
from fractal_server.app.runner.v2.models import Task
from fractal_server.app.runner.v2.models import WorkflowTask
from fractal_server.app.runner.v2.runner import execute_tasks_v2
from fractal_server.app.runner.v2.runner_functions import (
    MAX_PARALLELIZATION_LIST_SIZE,
)


@pytest.fixture()
def executor():
    with ThreadPoolExecutor() as e:
        yield e


@pytest.mark.xfail(reason="To be updated, in view of compound tasks")
@pytest.mark.parametrize("N", [100, 1000])
def test_max_parallelization_list_size(N: int, executor):
    parallelization_list = [
        dict(
            path=f"image-{i}",
            parameter=i,
        )
        for i in range(N)
    ]
    dataset = Dataset(
        parallelization_list=parallelization_list, zarr_dir="/tmp"
    )
    wf_task_list = [
        WorkflowTask(
            task=Task(
                name="task_parallel",
                task_type="parallel",
                function=dummy_task,
            ),
        )
    ]
    if N < MAX_PARALLELIZATION_LIST_SIZE:
        debug(N, "OK")
        execute_tasks_v2(
            wf_task_list=wf_task_list, dataset=dataset, executor=executor
        )
    else:
        with pytest.raises(ValueError) as e:
            execute_tasks_v2(
                wf_task_list=wf_task_list, dataset=dataset, executor=executor
            )
        debug(N, str(e.value))


def _copy_and_edit_image(
    path: str,
) -> dict[str, Any]:
    added_images = [
        dict(
            path=f"{path}_new",
            attributes=dict(processed=True),
        )
    ]
    return dict(added_images=added_images)


@pytest.mark.xfail(reason="To be updated, in view of compound tasks")
@pytest.mark.parametrize(
    "parallelization_list",
    [
        None,
        [
            dict(path="/tmp/mydir/plate.zarr/A/01/0"),
            dict(path="/tmp/mydir/plate.zarr/A/02/0"),
        ],
    ],
)
def test_image_attribute_propagation(
    parallelization_list: Optional[list[dict]],
    executor,
):
    zarr_dir = "/tmp/mydir"
    images_pre = [
        SingleImage(
            path=f"{zarr_dir}/plate.zarr/A/01/0",
            attributes=dict(plate="plate.zarr", well="A/01"),
        ),
        SingleImage(
            path=f"{zarr_dir}/plate.zarr/A/02/0",
            attributes=dict(plate="plate.zarr", well="A/02"),
        ),
    ]
    dataset_pre = Dataset(
        id=1,
        zarr_dir=zarr_dir,
        images=images_pre,
        parallelization_list=parallelization_list,
    )
    wf_task_list = [
        WorkflowTask(
            task=Task(
                name="task_parallel",
                task_type="parallel",
                function=_copy_and_edit_image,
            ),
        )
    ]
    dataset_post = execute_tasks_v2(
        wf_task_list=wf_task_list, dataset=dataset_pre, executor=executor
    )
    images_post = dataset_post.images

    debug(images_pre)
    debug(images_post)

    for image in images_post:
        print(f"Now validate {image}")
        if image.path == f"{zarr_dir}/plate.zarr/A/01/0_new":
            assert image.attributes["processed"] is True
            assert image.attributes["plate"] == "plate.zarr"
            assert image.attributes["well"] == "A/01"
        elif image.path == f"{zarr_dir}/plate.zarr/A/02/0_new":
            assert image.attributes["processed"] is True
            assert image.attributes["plate"] == "plate.zarr"
            assert image.attributes["well"] == "A/02"
        else:
            assert image.attributes["plate"] == "plate.zarr"
            assert "processed" not in image.attributes.keys()

    # wf_task_list_v1 = [
    #          WorkflowTask(
    #         task=Task(
    #             name="task_parallel",
    #             task_type="parallel",
    #             function=_copy_and_edit_image,
    #         ),
    #     )
    # ]
