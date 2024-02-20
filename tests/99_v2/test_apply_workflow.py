from devtools import debug
from tasks_for_tests import create_images_from_scratch
from tasks_for_tests import print_path
from tasks_for_tests import remove_images

from fractal_server.v2 import Dataset
from fractal_server.v2 import execute_tasks_v2
from fractal_server.v2 import Task
from fractal_server.v2 import WorkflowTask


def test_single_non_parallel_task():
    NEW_PATHS = ["/tmp/A/01/0", "/tmp/A/02/0", "/tmp/A/03/0"]
    dataset_in = Dataset(id=1)
    task_list = [
        WorkflowTask(
            task=Task(
                task_type="non_parallel",
                function=create_images_from_scratch,
            ),
            args=dict(new_paths=NEW_PATHS),
        )
    ]
    dataset_out = execute_tasks_v2(wf_task_list=task_list, dataset=dataset_in)
    debug(dataset_out.image_paths)
    assert set(dataset_out.image_paths) == set(NEW_PATHS)


def test_single_non_parallel_task_removed():
    IMAGES = [dict(path="/tmp/A/01/0"), dict(path="/tmp/A/02/0")]
    dataset_in = Dataset(id=1, images=IMAGES)
    task_list = [
        WorkflowTask(
            task=Task(
                task_type="non_parallel",
                function=remove_images,
            ),
            args=dict(removed_images_paths=["/tmp/A/01/0"]),
        )
    ]
    dataset_out = execute_tasks_v2(wf_task_list=task_list, dataset=dataset_in)
    debug(dataset_out.image_paths)
    assert dataset_out.image_paths == ["/tmp/A/02/0"]


def test_single_parallel_task_no_parallization_list():
    """This is currently not very useful"""
    IMAGES = [dict(path="/tmp/A/01/0"), dict(path="/tmp/A/02/0")]
    dataset_in = Dataset(id=1, images=IMAGES)
    task_list = [
        WorkflowTask(
            task=Task(
                task_type="parallel",
                function=print_path,
            )
        )
    ]
    dataset_out = execute_tasks_v2(wf_task_list=task_list, dataset=dataset_in)
    debug(dataset_out.image_paths)
