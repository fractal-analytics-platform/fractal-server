from fractal_server.app.models import Task
from fractal_server.app.runner._common import _task_needs_image_list
from fractal_server.app.runner._common import TaskParameters
from fractal_server.app.runner._common import trim_TaskParameters


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

    # For two specific tasks (see test_task_needs_image_list), history is
    # removed but metadata are preserved

    # Case 1
    new_taskpar = trim_TaskParameters(
        old_taskpar, Task(name="Copy OME-Zarr structure")
    )
    for key in ["input_paths", "output_path", "metadata"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)
    assert new_taskpar.history == []

    # Case 2
    new_taskpar = trim_TaskParameters(
        old_taskpar, Task(name="Convert Metadata Components from 2D to 3D")
    )
    for key in ["input_paths", "output_path", "metadata"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)
    assert new_taskpar.history == []

    # For generic tasks, both history and metadata["image"] are removed
    new_taskpar = trim_TaskParameters(old_taskpar, Task(name="task name"))
    for key in ["input_paths", "output_path"]:
        assert getattr(old_taskpar, key) == getattr(new_taskpar, key)
    assert new_taskpar.history == []
    assert new_taskpar.metadata == dict(well=["a"])
    assert "image" not in new_taskpar.metadata.keys()
