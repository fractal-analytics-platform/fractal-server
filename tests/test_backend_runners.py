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
import logging
import os

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
        request.getfixturevalue("cfut_jobs_finished")
        monkey_slurm_user = request.getfixturevalue("monkey_slurm_user")

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

    # Create working folder(s)
    if backend == "local":
        workflow_dir = tmp777_path / "server"  # OK 777 here
        workflow_dir_user = workflow_dir
        umask = os.umask(0)
        workflow_dir.mkdir(parents=True, mode=0o700)
        os.umask(umask)
    elif backend == "slurm":
        workflow_dir, workflow_dir_user = request.getfixturevalue(
            "slurm_working_folders"
        )  # noqa

    # Prepare backend-specific arguments
    logger_name = "job_logger"
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=workflow_dir / "job.log",
        level=logging.DEBUG,
    )
    kwargs = dict(
        workflow=wf,
        input_paths=[str(workflow_dir)],
        output_path=str(tmp777_path),  # OK 777 here
        input_metadata={},
        logger_name=logger_name,
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
    )
    if backend == "slurm":
        kwargs["slurm_user"] = monkey_slurm_user

    # process workflow
    metadata = await process_workflow(**kwargs)

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
    files = [f.name for f in workflow_dir.glob("*")] + [
        f.name for f in workflow_dir_user.glob("*")
    ]
    assert "0.args.json" in files
    assert "0.err" in files
    assert "0.out" in files
    assert "0.metadiff.json" in files

    with (workflow_dir_user / "0.args.json").open("r") as f:
        debug(workflow_dir_user / "0.args.json")
        args = f.read()
        debug(args)
        assert "logger_name" not in args
    if backend == "slurm":
        slurm_job_id = 2  # This may change if you change the test
        assert f"0.slurm.{slurm_job_id}.err" in files
        assert f"0.slurm.{slurm_job_id}.out" in files
