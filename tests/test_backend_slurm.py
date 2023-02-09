import os
import shlex
import subprocess
from concurrent.futures import Executor
from itertools import product
from pathlib import Path
from typing import Callable

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue
from .fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._slurm import SlurmConfig
from fractal_server.app.runner._slurm._subprocess_run_as_user import (
    _mkdir_as_user,
)
from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor
from fractal_server.app.runner.common import JobExecutionError
from fractal_server.tasks import dummy as dummy_module
from fractal_server.tasks import dummy_parallel as dummy_parallel_module


def _define_and_create_folders(root_path: Path, user: str):

    # Define working folders
    server_working_dir = root_path / "server"
    user_working_dir = root_path / "user"

    # Create server working folder
    umask = os.umask(0)
    server_working_dir.mkdir(parents=True, mode=0o755)
    os.umask(umask)

    # Create user working folder
    _mkdir_as_user(folder=str(user_working_dir), user=user)

    return (server_working_dir, user_working_dir)


def submit_and_ignore_exceptions(
    executor: Executor, fun: Callable, *args, **kwargs
):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(f"Ignored exception: {str(e)}")


def test_missing_slurm_user():
    with pytest.raises(TypeError):
        FractalSlurmExecutor()
    with pytest.raises(RuntimeError):
        FractalSlurmExecutor(slurm_user=None)


def test_submit_pre_command(fake_process, tmp_path, cfut_jobs_finished):
    """
    GIVEN a FractalSlurmExecutor
    WHEN it is initialised with a slurm_user
    THEN the sbatch call contains the sudo pre-command
    """
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])
    fake_process.register(["squeue", fake_process.any()])

    slurm_user = "some-fake-user"

    with FractalSlurmExecutor(
        slurm_user=slurm_user,
        working_dir=tmp_path,
        working_dir_user=tmp_path,
    ) as executor:
        submit_and_ignore_exceptions(executor, lambda: None)

    # Convert from deque to list, and apply shlex.join
    call_strings = [shlex.join(call) for call in fake_process.calls]
    debug(call_strings)

    # The first subprocess command in FractalSlurmExecutor (which fails, and
    # then stops the execution via submit_and_ignore_exceptions) is an `ls`
    # command to check that a certain folder exists. This will change if we
    # remove this check from FractalSlurmExecutor, or if another subprocess
    # command is called before the `ls` one.
    target = f"sudo --non-interactive -u {slurm_user} ls"
    debug([target in call for call in call_strings])
    assert any([target in call for call in call_strings])


def test_unit_sbatch_script_readable(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    """
    GIVEN a batch script written to file by the slurm executor
    WHEN a different user tries to read it
    THEN it has all the permissions needed
    """

    folders = _define_and_create_folders(tmp777_path, monkey_slurm_user)
    server_working_dir, user_working_dir = folders[:]

    SBATCH_SCRIPT = "test"
    with FractalSlurmExecutor(
        working_dir=server_working_dir,
        working_dir_user=user_working_dir,
        slurm_user=monkey_slurm_user,
    ) as executor:
        f = executor.write_batch_script(
            SBATCH_SCRIPT, dest=server_working_dir / "script.sbatch"
        )

    out = subprocess.run(
        shlex.split(
            f"sudo --non-interactive -u {monkey_slurm_user} sbatch {f}"
        ),
        capture_output=True,
        text=True,
    )
    debug(out.stderr)
    assert out.returncode == 1
    assert "Unable to open file" not in out.stderr
    assert "This does not look like a batch script" in out.stderr


def test_slurm_executor_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    """
    GIVEN a docker slurm cluster and a FractalSlurmExecutor executor
    WHEN a function is submitted to the executor, as a given user
    THEN the result is correctly computed
    """

    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=4,
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_slurm_executor_map(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=4,
    ) as executor:
        result_generator = executor.map(lambda x: 2 * x, range(4))
        results = list(result_generator)
        debug(results)
        assert results == [2 * x for x in range(4)]


def test_slurm_executor_submit_separate_folders(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    """
    Same as test_slurm_executor, but with two folders:
    * server_working_dir is owned by the server user and has 755 permissions
    * user_working_dir is owned the user and had default permissions
    """

    folders = _define_and_create_folders(tmp777_path, monkey_slurm_user)
    server_working_dir, user_working_dir = folders[:]

    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=server_working_dir,
        working_dir_user=user_working_dir,
        slurm_poll_interval=4,
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_slurm_executor_submit_and_scancel(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    """
    GIVEN a docker slurm cluster and a FractalSlurmExecutor executor
    WHEN a function is submitted to the executor (as a given user) and then the
         SLURM job is immediately canceled
    THEN the error is correctly captured
    """

    import time

    def wait_and_return():
        time.sleep(60)
        return 42

    folders = _define_and_create_folders(tmp777_path, monkey_slurm_user)
    server_working_dir, user_working_dir = folders[:]

    with pytest.raises(JobExecutionError) as e:
        with FractalSlurmExecutor(
            slurm_user=monkey_slurm_user,
            working_dir=server_working_dir,
            working_dir_user=user_working_dir,
            debug=True,
            keep_logs=True,
            slurm_poll_interval=4,
        ) as executor:
            fut = executor.submit(wait_and_return)
            debug(fut)

            # Wait until the SLURM job goes from PENDING to RUNNING
            while True:
                squeue_output = run_squeue(
                    squeue_format="%i %u %T", header=False
                )
                debug(squeue_output)
                if "RUNNING" in squeue_output:
                    break
                time.sleep(1)

            # Scancel all jobs of the current SLURM user
            scancel_all_jobs_of_a_slurm_user(
                slurm_user=monkey_slurm_user, show_squeue=True
            )

            # Calling result() forces waiting for the result, which in this
            # test raises an exception
            fut.result()

    debug(str(e.type))
    debug(str(e.value))
    debug(str(e.traceback))

    debug(e.value.assemble_error())

    assert "CANCELLED" in e.value.assemble_error()
    # Since we waited for the job to be RUNNING, both the SLURM stdout and
    # stderr files should exist
    assert "missing" not in e.value.assemble_error()


def test_unit_slurm_config():
    """
    GIVEN the Slurm configuration class
    WHEN it is instantiated with a mix of attribute names and attribute aliases
    THEN
        * The object is correctly instantiated, regardless of whether the
          parameters were passed by attribute name or by alias

        Furthermore, when `to_sbatch()` is called
        * the object's attributes are correctly returned as a list of strings
        * the `name` attribute is not included
    """
    ARGS = {
        "partition": "partition",
        "cpus-per-task": 4,
        "extra_lines": ["#SBATCH extra line 0", "#SBATCH extra line 1"],
    }
    sc = SlurmConfig(**ARGS)
    debug(sc)
    sbatch_lines = sc.to_sbatch()
    assert len(sbatch_lines) == len(ARGS) + 1
    debug(sbatch_lines)
    for line in sbatch_lines:
        assert line.startswith("#SBATCH")
        # check that '_' in field names is never used, but changed to '_'
        assert "_" not in line


@pytest.mark.parametrize(
    ("slurm_config_key", "task"),
    product(
        ("default", "low"),
        (
            MockTask(
                name="task serial",
                command=f"python {dummy_module.__file__}",
            ),
            MockTask(
                name="task parallel",
                command=f"python {dummy_parallel_module.__file__}",
                parallelization_level="index",
            ),
        ),
    ),
)
def test_sbatch_script_slurm_config(
    tmp_path,
    slurm_config,
    slurm_config_key,
    task,
    cfut_jobs_finished,
):
    """
    GIVEN
        * a workflow submitted via `recursive_task_submission`
        * a valid slurm configuration file` defining `default` and `low`
          configurations
    WHEN a `submit_setup_call` is set`that customises each task's configuration
    THEN the configuration options are correctly set in the sbatch script
    """
    from fractal_server.app.runner.common import TaskParameters
    from fractal_server.app.runner._common import recursive_task_submission
    from fractal_server.app.runner._slurm import set_slurm_config

    task_list = [
        MockWorkflowTask(
            task=task,
            arguments=dict(message="test"),
            order=0,
            executor=slurm_config_key,
        )
    ]
    logger_name = "job_logger_recursive_task_submission_step0"
    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata={"index": ["a", "b"]},
    )

    # Assign a non existent slurm_user so that the sudo call will fail with a
    # FileNotFoundError. This will allow inspection of the sbatch script file.
    sbatch_init_lines = [
        "export FOO=bar",
        "#SBATCH --common-non-existent-option",
    ]
    with FractalSlurmExecutor(
        slurm_user="NO_USER",
        working_dir=tmp_path,
        working_dir_user=tmp_path,
        common_script_lines=sbatch_init_lines,
    ) as executor:

        try:
            recursive_task_submission(
                executor=executor,
                task_list=task_list,
                task_pars=task_pars,
                workflow_dir=tmp_path,
                workflow_dir_user=tmp_path,
                submit_setup_call=set_slurm_config,
                logger_name=logger_name,
            )
        except subprocess.CalledProcessError as e:
            assert "unknown user NO_USER" in e.stderr.decode(
                "utf-8"
            ) or "unknown user: NO_USER" in e.stderr.decode("utf-8")
            sbatch_file = e.cmd[-1]
            debug(sbatch_file)
        with open(sbatch_file, "r") as f:
            sbatch_script_lines = f.read().split("\n")
            debug(sbatch_script_lines)

        expected_mem = f"mem={slurm_config[slurm_config_key]['mem']}"
        debug(expected_mem)
        assert next(
            (line for line in sbatch_script_lines if expected_mem in line),
            False,
        )

        job_name = next(
            (line for line in sbatch_script_lines if "--job-name" in line),
            False,
        )
        assert job_name
        assert len(job_name.split()[-1]) == len(task.name)

        sbatch_script = "".join(sbatch_script_lines)
        for line in sbatch_init_lines:
            assert line in sbatch_script_lines
        debug(sbatch_script)
        if "task_parallel" in sbatch_script:
            output_line = next(
                (line for line in sbatch_script_lines if "--output" in line),
                False,
            ).strip()
            error_line = next(
                (line for line in sbatch_script_lines if "--error" in line),
                False,
            ).strip()

            # output and error filenames for parallel tasks should contain the
            # `{task_order}_par_{component}` tag
            debug(output_line)
            assert "_par_" in output_line
            assert "_par_" in error_line
