from concurrent.futures import Executor
from itertools import product
from typing import Callable

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._slurm import SlurmConfig
from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor
from fractal_server.tasks import dummy as dummy_module
from fractal_server.tasks import dummy_parallel as dummy_parallel_module


def submit(executor: Executor, fun: Callable, *args, **kwargs):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(e)


@pytest.mark.parametrize(("username"), [None, "my_user"])
def test_submit_pre_command(fake_process, username, tmp_path):
    """
    GIVEN a FractalSlurmExecutor
    WHEN it is initialised with / without a username
    THEN the sbatch call contains / does not contain the sudo pre-command
    """
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])

    with FractalSlurmExecutor(
        username=username, script_dir=tmp_path
    ) as executor:
        submit(executor, lambda: None)

    debug(fake_process.calls)
    call = fake_process.calls.pop()
    assert "sbatch" in call
    if username:
        assert f"sudo --non-interactive -u {username}" in call


def test_unit_sbatch_script_readable(monkey_slurm, tmp777_path):
    """
    GIVEN a batch script written to file by the slurm executor
    WHEN a differnt user tries to read it
    THEN it has all the permissions needed
    """
    from fractal_server.app.runner._slurm.executor import write_batch_script
    import subprocess
    import shlex

    SBATCH_SCRIPT = "test"
    f = write_batch_script(SBATCH_SCRIPT, script_dir=tmp777_path)

    out = subprocess.run(
        shlex.split(f"sudo --non-interactive -u test01 sbatch {f}"),
        capture_output=True,
        text=True,
    )
    debug(out.stderr)
    assert out.returncode == 1
    assert "Unable to open file" not in out.stderr
    assert "This does not look like a batch script" in out.stderr


@pytest.mark.parametrize("username", [None, "test01"])
def test_slurm_executor(username, monkey_slurm, tmp777_path):
    """
    GIVEN a slurm cluster in a docker container
    WHEN a function is submitted to the cluster executor, as a given user
    THEN the result is correctly computed
    """

    with FractalSlurmExecutor(
        script_dir=tmp777_path, username=username
    ) as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_unit_slurm_config():
    """
    GIVEN a Slurm configuration object
    WHEN the `to_sbatch()` method is called
    THEN
        * the object's attributes are correctly returned as a list of strings
        * the `name` attribute is not included
    """
    sc = SlurmConfig(name="name", partition="partition")
    sbatch_lines = sc.to_sbatch()
    debug(sbatch_lines)
    for line in sbatch_lines:
        assert line.startswith("#SBATCH")
        assert "name" not in line


@pytest.mark.parametrize(
    ("slurm_config_key", "task"),
    product(
        ("default", "low"),
        (
            MockTask(
                name="task_serial",
                command=f"python {dummy_module.__file__}",
            ),
            MockTask(
                name="task_parallel",
                command=f"python {dummy_parallel_module.__file__}",
                parallelization_level="index",
            ),
        ),
    ),
)
def test_sbatch_script_slurm_config(
    tmp_path, slurm_config, slurm_config_key, task
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
        logger_name=logger_name,
    )

    # Assign a non existent username so that the sudo call will fail with a
    # FileNotFoundError. This will allow inspection of the sbatch script file.
    with FractalSlurmExecutor(username="NO_USER") as executor:
        try:
            recursive_task_submission(
                executor=executor,
                task_list=task_list,
                task_pars=task_pars,
                workflow_dir=tmp_path,
                submit_setup_call=set_slurm_config,
            )
        except FileNotFoundError as e:
            sbatch_file = str(e).split()[-1].strip("'")
        with open(sbatch_file, "r") as f:
            sbatch_script_lines = f.readlines()
            debug(sbatch_script_lines)

        expected_mem = f"mem={slurm_config[slurm_config_key]['mem']}"
        debug(expected_mem)
        assert next(
            (line for line in sbatch_script_lines if expected_mem in line),
            False,
        )
