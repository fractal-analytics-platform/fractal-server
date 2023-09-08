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

import pytest  # type: ignore[import]
from devtools import debug

from fractal_server.app.models import Workflow
from fractal_server.app.runner import _backends
from fractal_server.app.runner.common import close_job_logger
from fractal_server.logger import set_logger


def _extract_job_id_from_filename(filenames, pre, post) -> int:
    # Find SLURM-job ID from filename
    _filename = next(
        f for f in filenames if f.startswith(pre) and f.endswith(post)
    )
    debug(_filename)
    assert _filename
    slurm_job_id = int(_filename.strip(pre).strip(post))
    return slurm_job_id


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
    testdata_path,
    override_settings_factory,
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
        request.getfixturevalue("cfut_jobs_finished")
        monkey_slurm_user = request.getfixturevalue("monkey_slurm_user")
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )
        request.getfixturevalue("slurm_config")

    process_workflow = _backends[backend]

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user=user)

    # Add dummy task as a Task
    tk_dummy = collect_packages[0]
    tk_dummy_parallel = collect_packages[1]
    debug(tk_dummy)
    debug(tk_dummy_parallel)

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
    logger_name = f"job_logger_{backend}"
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=str(workflow_dir / "job.log"),
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
    try:
        metadata = await process_workflow(**kwargs)
    except Exception as e:
        debug(str(e))
        logging.error(f"process_workflow for {backend=} failed.")
        logging.error(f"Original error: {str(e)}")
        raise e

    close_job_logger(logger)
    debug(metadata)
    assert "dummy" in metadata
    for event in metadata["HISTORY_NEXT"]:
        assert event["status"] == "done"
    event0, event1, event2 = metadata["HISTORY_NEXT"]
    assert event0["workflowtask"]["task"]["name"] == tk_dummy.name
    assert event1["workflowtask"]["task"]["name"] == tk_dummy.name
    assert event2["workflowtask"]["task"]["name"] == tk_dummy_parallel.name
    assert event2["parallelization"]["component_list"] == ["0", "1", "2"]

    # Check that the correct files are present in workflow_dir
    files_server = [f.name for f in workflow_dir.glob("*")]
    files_user = [f.name for f in workflow_dir_user.glob("*")]
    debug(sorted(files_server))
    debug(sorted(files_user))

    # Check some backend-independent files
    assert "0.args.json" in files_server
    assert "0.err" in files_server
    assert "0.out" in files_server
    assert "0.metadiff.json" in files_server
    assert "2_par_0.args.json" in files_server
    assert "2_par_0.err" in files_server
    assert "2_par_0.out" in files_server
    assert "2_par_0.metadiff.json" in files_server
    with (workflow_dir_user / "0.args.json").open("r") as f:
        debug(workflow_dir_user / "0.args.json")
        args = f.read()
        debug(args)
        assert "logger_name" not in args
    # Check some backend-specific files
    # NOTE: the logic to retrieve the job ID is not the most elegant, but
    # it works (both for when a single or two SLURM backends are tested)
    if backend == "slurm":
        # Files related to (non-parallel) WorkflowTask 0
        assert "0_slurm_submit.sbatch" in files_server
        slurm_job_id = _extract_job_id_from_filename(
            files_server, pre="0_slurm_", post=".err"
        )
        assert slurm_job_id
        assert f"0_slurm_{slurm_job_id}.err" in files_server
        assert f"0_slurm_{slurm_job_id}.out" in files_server
        # Files related to (parallel) WorkflowTask 2
        assert "2_batch_000000_slurm_submit.sbatch" in files_server
        slurm_job_id = slurm_job_id + 2
        assert f"2_batch_000000_slurm_{slurm_job_id}.err" in files_server
        assert f"2_batch_000000_slurm_{slurm_job_id}.out" in files_server
