from concurrent.futures import Executor
from pathlib import Path

import pytest
from devtools import debug
from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import DatasetV2Mock
from v2_mock_models import WorkflowTaskV2Mock

from fractal_server.app.runner.v2.runner import execute_tasks_v2


def test_dummy_insert_single_image(
    tmp_path: Path, executor: Executor, fractal_tasks_mock_venv
):
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
