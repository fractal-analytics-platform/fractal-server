import json

from fractal_server.app.models import Task
from fractal_server.app.runner._common import _task_needs_image_list
from fractal_server.app.runner.common import write_args_file


def test_write_args_file(tmp_path):

    ARGS = dict(metadata=dict(image=[1, 2, 3], well=[1]))
    STRIPPED_ARGS = dict(metadata=dict(well=[1]))
    args_path = tmp_path / "0.json"
    write_args_file(ARGS, path=args_path, include_image_list=True)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == ARGS
    args_path = tmp_path / "1.json"
    write_args_file(ARGS, path=args_path, include_image_list=False)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == STRIPPED_ARGS

    ARGS = dict(metadata=dict(well=[1]))
    args_path = tmp_path / "3.json"
    write_args_file(ARGS, path=args_path, include_image_list=True)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == ARGS
    args_path = tmp_path / "4.json"
    write_args_file(ARGS, path=args_path, include_image_list=False)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == ARGS

    ARGS = dict(something="else", metadata=[])
    args_path = tmp_path / "5.json"
    write_args_file(ARGS, path=args_path, include_image_list=True)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == ARGS
    args_path = tmp_path / "6.json"
    write_args_file(ARGS, path=args_path, include_image_list=False)
    with args_path.open("r") as f:
        args = json.load(f)
        assert args == ARGS


def test_task_needs_image_list():
    assert not _task_needs_image_list(Task(name="name", source="source"))
    assert not _task_needs_image_list(
        Task(
            name="name",
            source="pip_remote:fractal_tasks_core:...",
        )
    )
    assert not _task_needs_image_list(
        Task(
            name="Copy OME-Zarr structure",
            source="source",
        )
    )
    assert _task_needs_image_list(
        Task(
            name="Copy OME-Zarr structure",
            source="pip_remote:fractal_tasks_core:...",
        )
    )
