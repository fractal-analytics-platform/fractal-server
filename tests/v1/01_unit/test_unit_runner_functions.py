from copy import deepcopy

from fractal_server.app.models.v1 import Task
from fractal_server.app.runner.v1._common import _task_needs_image_list
from fractal_server.app.runner.v1._common import TaskParameters
from fractal_server.app.runner.v1._common import trim_TaskParameters


def test_task_needs_image_list():
    assert not _task_needs_image_list(Task(name="name"))
    assert _task_needs_image_list(Task(name="Copy OME-Zarr structure"))
    assert _task_needs_image_list(
        Task(name="Convert Metadata Components from 2D to 3D")
    )


def test_trim_TaskParameters():
    old_taskpar = TaskParameters(
        input_paths=["a", "b"],
        output_path="c",
        history=[dict(key="value")],
        metadata=dict(well=["a"], image=["a1", "a2"]),
    )
    old_taskpar_deepcopy = deepcopy(old_taskpar)

    # For two specific tasks (see test_task_needs_image_list) and for
    # non-parallel tasks, metadata are preserved

    # Case 1
    new_taskpar = trim_TaskParameters(
        old_taskpar,
        Task(
            name="Copy OME-Zarr structure",
            meta=dict(parallelization_level="image"),
        ),
    )
    for key in ["input_paths", "output_path", "history", "metadata"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)

    # Verify that input was not modified
    assert old_taskpar == old_taskpar_deepcopy

    # Case 2
    new_taskpar = trim_TaskParameters(
        old_taskpar,
        Task(
            name="Convert Metadata Components from 2D to 3D",
            meta=dict(parallelization_level="image"),
        ),
    )
    for key in ["input_paths", "output_path", "history", "metadata"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)

    # Verify that input was not modified
    assert old_taskpar == old_taskpar_deepcopy

    # Case 3
    new_taskpar = trim_TaskParameters(
        old_taskpar, Task(name="name", meta=dict(key="value"))
    )
    for key in ["input_paths", "output_path", "history", "metadata"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)

    # Verify that input was not modified
    assert old_taskpar == old_taskpar_deepcopy

    # For generic *parallel* tasks, both history and metadata["image"] are
    # removed
    new_taskpar = trim_TaskParameters(
        old_taskpar,
        Task(name="task name", meta=dict(parallelization_level="image")),
    )
    for key in ["input_paths", "output_path"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)
    assert new_taskpar.metadata == dict(well=["a"])
    assert "image" not in new_taskpar.metadata.keys()
    assert new_taskpar.history == []

    # Verify that input was not modified
    assert old_taskpar == old_taskpar_deepcopy
