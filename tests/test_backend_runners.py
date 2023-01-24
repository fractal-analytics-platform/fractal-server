"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import datetime
import logging

import pytest
from devtools import debug

from fractal_server.app.models import Workflow
from fractal_server.app.runner import _backends
from fractal_server.app.runner.common import close_job_logger
from fractal_server.utils import set_logger


backends_available = list(_backends.keys())


@pytest.mark.parametrize(
    "backend",
    backends_available,
)
async def test_runner(
    db,
    project_factory,
    MockCurrentUser,
    collect_packages,
    tmp777_path,
    backend,
    request,
):
    """
    GIVEN a non-trivial workflow
    WHEN the workflow is processed
    THEN the tasks are correctly executed
    """
    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("slurm_config")

    process_workflow = _backends[backend]

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user=user)

    # Add dummy task as a Task
    tk_dummy = collect_packages[0]
    tk_dummy_parallel = collect_packages[1]

    # Create a workflow with the dummy task as member
    wf = Workflow(name="wf", project_id=prj.id)

    db.add(wf)
    await db.commit()
    await db.refresh(wf)

    await wf.insert_task(tk_dummy.id, db=db, args=dict(message="task 0"))
    await wf.insert_task(tk_dummy.id, db=db, args=dict(message="task 1"))
    await wf.insert_task(
        tk_dummy_parallel.id, db=db, args=dict(message="task 2")
    )
    await db.refresh(wf)

    debug(wf)

    # process workflow
    logger_name = "job_logger"
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=tmp777_path / "job.log",
        level=logging.DEBUG,
    )
    metadata = await process_workflow(
        workflow=wf,
        input_paths=[tmp777_path / "*.txt"],
        output_path=tmp777_path / "out.json",
        input_metadata={},
        logger_name=logger_name,
        workflow_dir=tmp777_path,
    )
    close_job_logger(logger)
    debug(metadata)
    assert "dummy" in metadata
    assert "dummy" in metadata
    assert metadata["history"] == [
        tk_dummy.name,
        tk_dummy.name,
        f"{tk_dummy_parallel.name}: ['0', '1', '2']",
    ]

    # Check that the correct files are present in workflow_dir
    files = [f.name for f in tmp777_path.glob("*")]
    assert "0.args.json" in files
    assert "0.err" in files
    assert "0.out" in files
    assert "0.metadiff.json" in files
    if backend == "slurm":
        slurm_job_id = 2  # This may change if you change the test
        assert f"0.slurm.{slurm_job_id}.err" in files
        assert f"0.slurm.{slurm_job_id}.out" in files
    with (tmp777_path / "0.args.json").open("r") as f:
        debug(tmp777_path / "0.args.json")
        args = f.read()
        debug(args)
        assert "logger_name" not in args


@pytest.mark.skip(reason="Skip in favor of test_call_parallel_task_max_tasks")
@pytest.mark.parametrize("max_tasks", [None, 1])
async def test_runner_max_number_tasks(
    db,
    project_factory,
    MockCurrentUser,
    collect_packages,
    tmp777_path,
    request,
    override_settings_factory,
    max_tasks,
):
    """
    GIVEN A workflow with a single task, parallelized over two components
    WHEN This workflow is submitted through the local backend
    THEN The FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW env variable is used
         correctly
    """

    SLEEP_TIME = 1
    INPUT_METADATA = dict(index=["0", "1"])
    process_workflow = _backends["local"]
    override_settings_factory(FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW=max_tasks)

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user=user)

    # Create a workflow with the dummy_parallel task
    task_dummy_parallel = collect_packages[1]
    wf = Workflow(name="wf", project_id=prj.id)
    db.add(wf)
    await db.commit()
    await db.refresh(wf)
    await wf.insert_task(
        task_dummy_parallel.id,
        db=db,
        args=dict(message="task 2", sleep_time=SLEEP_TIME),
    )
    await db.refresh(wf)
    debug(wf)

    # Execute workflow
    metadata = await process_workflow(
        workflow=wf,
        input_paths=[tmp777_path / "*.txt"],
        output_path=tmp777_path / "out.json",
        input_metadata=INPUT_METADATA,
        logger_name="my-logs",
        workflow_dir=tmp777_path,
    )
    debug(metadata)

    # Check that the two tasks were submitted at the appropriate time,
    # depending on FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW. NOTE: the log parsing
    # and log-to-datetime conversion may easily break if we change the logs
    # format
    with (tmp777_path / "0_par_0.err").open("r") as f:
        first_log_task_0 = f.readlines()[0]
    with (tmp777_path / "0_par_1.err").open("r") as f:
        first_log_task_1 = f.readlines()[0]
    debug(first_log_task_0)
    debug(first_log_task_1)
    assert "; INFO; ENTERING" in first_log_task_0
    assert "; INFO; ENTERING" in first_log_task_1
    # Parse times
    fmt = "%Y-%m-%d %H:%M:%S,%f"
    time_start_task_0 = datetime.datetime.strptime(
        first_log_task_0.split("; INFO; ENTERING")[0], fmt
    )
    time_start_task_1 = datetime.datetime.strptime(
        first_log_task_1.split("; INFO; ENTERING")[0], fmt
    )
    debug(time_start_task_0)
    debug(time_start_task_1)
    # Check time difference
    diff = (time_start_task_1 - time_start_task_0).total_seconds()
    debug(diff)
    if max_tasks == 1:
        assert diff >= SLEEP_TIME
    else:
        assert diff < SLEEP_TIME
