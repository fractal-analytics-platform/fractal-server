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
import json
import logging
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._common import _call_command_wrapper
from fractal_server.app.runner._common import call_parallel_task
from fractal_server.app.runner._common import call_single_task
from fractal_server.app.runner._common import recursive_task_submission
from fractal_server.app.runner.common import close_job_logger
from fractal_server.app.runner.common import TaskParameters
from fractal_server.tasks import dummy as dummy_module
from fractal_server.tasks import dummy_parallel as dummy_parallel_module
from fractal_server.utils import set_logger


async def test_command_wrapper(tmp_path):
    OUT_PATH = tmp_path / "out"
    ERR_PATH = tmp_path / "err"
    with ThreadPoolExecutor() as executor:
        future = executor.submit(
            _call_command_wrapper,
            f"ls -al {dummy_module.__file__}",
            stdout=OUT_PATH,
            stderr=ERR_PATH,
        )
    future.result()

    with OUT_PATH.open("r") as fout:
        assert dummy_module.__file__ in fout.read()


def test_call_single_task(tmp_path):
    task = MockWorkflowTask(
        task=MockTask(name="task0", command=f"python {dummy_module.__file__}"),
        arguments=dict(message="test"),
        order=0,
    )
    logger_name = "test_logger_call_single_task"
    job_logger = set_logger(
        logger_name=logger_name,
        log_file_path=tmp_path / "job.log",
        level=logging.DEBUG,
    )
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata={},
    )

    debug(task)

    out = call_single_task(
        task=task, task_pars=task_pars, workflow_dir=tmp_path
    )
    close_job_logger(job_logger)
    debug(out)
    assert isinstance(out, TaskParameters)
    # check specific of the dummy task
    assert out.metadata["dummy"] == "dummy 0"


def test_recursive_task_submission_step0(tmp_path):
    """
    GIVEN a workflow with a single task
    WHEN it is passed to the recursive task submission
    THEN it is correctly executed, i.e., step 0 of the induction
    """
    INDEX = 666
    task_list = [
        MockWorkflowTask(
            task=MockTask(
                name="task0", command=f"python {dummy_module.__file__}"
            ),
            arguments=dict(message="test", index=INDEX),
            order=0,
        )
    ]
    logger_name = "job_logger_recursive_task_submission_step0"
    job_logger = set_logger(
        logger_name=logger_name,
        log_file_path=tmp_path / "job.log",
        level=logging.DEBUG,
    )
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata={},
    )

    with ThreadPoolExecutor() as executor:
        res = recursive_task_submission(
            executor=executor,
            task_list=task_list,
            task_pars=task_pars,
            workflow_dir=tmp_path,
            logger_name=logger_name,
        )
        debug(res.result())
        assert res.result().metadata["dummy"] == f"dummy {INDEX}"
    close_job_logger(job_logger)


def test_recursive_parallel_task_submission_step0(tmp_path):
    """
    GIVEN a workflow with a single parallel task
    WHEN it is passed to the recursive task submission
    THEN it is correctly executed, i.e., step 0 of the induction
    """
    LIST_INDICES = ["something/0", "something/1"]
    MESSAGE = "test message"
    MOCKPARALLELTASK_NAME = "This is just a name"
    task_list = [
        MockWorkflowTask(
            task=MockTask(
                name=MOCKPARALLELTASK_NAME,
                command=f"python {dummy_parallel_module.__file__}",
                parallelization_level="index",
            ),
            arguments=dict(message=MESSAGE),
            order=0,
        )
    ]
    logger_name = "job_logger_recursive_parallel_task_submission_step0"
    job_logger = set_logger(
        logger_name=logger_name,
        log_file_path=tmp_path / "job.log",
        level=logging.DEBUG,
    )
    output_path = tmp_path / "output/*.json"
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=output_path,
        metadata={"index": LIST_INDICES},
    )

    debug(task_list)
    debug(task_pars)

    with ThreadPoolExecutor() as executor:
        res = recursive_task_submission(
            executor=executor,
            task_list=task_list,
            task_pars=task_pars,
            workflow_dir=tmp_path,
            logger_name=logger_name,
        )
        debug(res.result())
        assert MOCKPARALLELTASK_NAME in res.result().metadata["history"][0]
    close_job_logger(job_logger)

    # Validate results
    assert output_path.parent.exists()
    output_files = list(output_path.parent.glob("*"))
    debug(output_files)
    assert len(output_files) == len(LIST_INDICES)

    for output_file in output_files:
        with output_file.open("r") as fin:
            data = json.load(fin)
        safe_component = data["component"].replace(" ", "_")
        safe_component = safe_component.replace(".", "_").replace("/", "_")
        assert output_file.name == f"{safe_component}.result.json"
        assert data["message"] == MESSAGE


def test_recursive_task_submission_inductive_step(tmp_path):
    """
    GIVEN a workflow with two or more tasks
    WHEN it is passed to the recursive task submission
    THEN it is correctly executed, i.e., n => n+1
    """
    TASK_NAME = "task0"
    METADATA_0 = {}
    METADATA_1 = dict(
        dummy="dummy 0", index=["0", "1", "2"], history=[TASK_NAME]
    )  # dummy task output

    task_list = [
        MockWorkflowTask(
            task=MockTask(
                name=TASK_NAME, command=f"python {dummy_module.__file__}"
            ),
            arguments=dict(message="test 0", index=0),
            order=0,
        ),
        MockWorkflowTask(
            task=MockTask(
                name="task1", command=f"python {dummy_module.__file__}"
            ),
            arguments=dict(message="test 1", index=1),
            order=1,
        ),
    ]
    logger_name = "job_logger_recursive_task_submission_inductive_step"
    job_logger = set_logger(
        logger_name=logger_name,
        log_file_path=tmp_path / "job.log",
        level=logging.DEBUG,
    )
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path / "output.json",
        metadata=METADATA_0,
    )

    with ThreadPoolExecutor() as executor:
        res = recursive_task_submission(
            executor=executor,
            task_list=task_list,
            task_pars=task_pars,
            workflow_dir=tmp_path,
            logger_name=logger_name,
        )
    close_job_logger(job_logger)

    output = res.result()
    debug(output)
    with open(output.output_path, "r") as f:
        data = json.load(f)
    debug(data)
    assert len(data) == 2
    assert data[0]["metadata"] == METADATA_0
    assert data[1]["metadata"] == METADATA_1


@pytest.mark.parametrize("max_tasks", [None, 1])
def test_call_parallel_task_max_tasks(
    tmp_path,
    max_tasks,
    override_settings_factory,
):
    """
    GIVEN A single task, parallelized over two components
    WHEN This task is executed on a ThreadPoolExecutor via call_parallel_task
    THEN The FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW env variable is used
         correctly
    """

    # Reset environment variable
    debug(max_tasks)
    override_settings_factory(FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW=max_tasks)

    # Prepare task
    SLEEP_TIME = 1
    task = MockWorkflowTask(
        task=MockTask(
            name="task0",
            command=f"python {dummy_parallel_module.__file__}",
            parallelization_level="index",
        ),
        arguments=dict(message="message", sleep_time=SLEEP_TIME),
        order=0,
    )
    debug(task)

    # Prepare task arguments (both as TaskParameters and as a dummy Future)
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata=dict(index=["0", "1"]),
    )
    debug(task_pars)
    task_pars_future = Future()
    task_pars_future.set_result(task_pars)
    debug(task_pars_future)

    # Execute task
    with ThreadPoolExecutor() as executor:
        future_metadata = call_parallel_task(
            executor=executor,
            task=task,
            task_pars_depend_future=task_pars_future,
            workflow_dir=tmp_path,
        )
    debug(tmp_path)
    debug(future_metadata)
    out = future_metadata.result()
    debug(out)
    assert isinstance(out, TaskParameters)

    # Check that the two tasks were submitted at the appropriate time,
    # depending on FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW. NOTE: the log parsing
    # and log-to-datetime conversion may easily break if we change the logs
    # format
    with (tmp_path / "0_par_0.err").open("r") as f:
        first_log_task_0 = f.readlines()[0]
    with (tmp_path / "0_par_1.err").open("r") as f:
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
