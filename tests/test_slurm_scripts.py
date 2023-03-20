import glob
import json
import math
import os
import shlex
import shutil
import subprocess
import time
from copy import copy

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue


def write_script(
    list_args: list[int],
    num_tasks_max_running: int,
    mem_per_task_MB: int,
    cpu_per_task: int,
    logdir: str,
    command: str,
    sleep_time: float,
):

    script = "\n".join(
        (
            "#!/bin/bash",
            "#SBATCH --partition=main",
            f"#SBATCH --err={logdir}/err",
            f"#SBATCH --out={logdir}/out",
            f"#SBATCH --cpus-per-task={cpu_per_task}",
            "\n",
        )
    )

    tmp_list_args = copy(list_args)
    while tmp_list_args:
        for ind in range(num_tasks_max_running):
            if tmp_list_args:
                arg = tmp_list_args.pop(0)  # take first element
                script += (
                    "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
                    f"--mem={mem_per_task_MB}MB "
                    f"{command} {arg} {sleep_time} {logdir} &\n"
                )
        script += "wait\n\n"

    return script


# A case consists of:
# * n_ftasks_tot
# * n_ftasks_per_script
# * n_parallel_ftasks_per_script
cases = []

# (1) Single script
# (1a) No parallelism
cases.append((4, 4, 1))
# (1b) Commensurable parallelism
cases.append((4, 4, 2))
cases.append((4, 4, 4))
# (1c) Incommensurable parallelism
cases.append((4, 4, 3))
# (2) Multiple scripts (commensurable)
# (2a) No parallelism
cases.append((8, 4, 1))
# (2b) Commensurable parallelism
cases.append((8, 4, 2))
cases.append((8, 4, 4))
# (2c) Incommensurable parallelism
cases.append((8, 4, 3))
# (3) Multiple scripts (incommensurable)
# (3a) No parallelism
cases.append((10, 4, 1))
# (3b) Commensurable parallelism
cases.append((10, 4, 2))
cases.append((10, 4, 4))
# (3c) Incommensurable parallelism
cases.append((10, 4, 3))


@pytest.mark.parametrize(
    "n_ftasks_tot,n_ftasks_per_script,n_parallel_ftasks_per_script",
    cases,
)
@pytest.mark.parametrize("cpu_per_task", [1, 3])
def test_slurm_script(
    n_ftasks_tot,
    n_ftasks_per_script,
    n_parallel_ftasks_per_script,
    cpu_per_task,
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
    testdata_path,
):
    """
    GIVEN
      An executable command to be parallelized over a `components` list of
      `n_tasks_per_script`.
    WHEN
      We do everything right...
    THEN
     * The number of scripts is equal to the ceiling of
       `n_ftasks_tot/n_ftasks_per_script`.
     * Each script includes no more than n_ftasks_per_script.
     * Inside each script, Fractal tasks are executed in parallel in groupd of
       size `n_parallel_ftasks_per_script`.
     * Both the primary batching (Fractal tasks -> scripts) and secondary
       batching (grouping of Fractal tasks into sub-batches for parallel
       execution) must work correctly also for incommensurable numbers.
     * Runtime of each SLURM job must correspond to the expected one, obtained
       as the ceiling of the ratio between the number of Fractal tasks and
       `n_parallel_ftasks_per_script`.


    Arguments:
        n_ftasks_tot:
            Total number of f-tasks to be run
        n_ftasks_per_script:
            Number of f-tasks for each submission script
        n_parallel_ftasks_per_script:
            Maximum number of f-tasks from the same script which run in
            parallel
    """

    # NOTE: by now we make everything 777

    assert n_parallel_ftasks_per_script <= n_ftasks_per_script
    assert n_ftasks_per_script <= n_ftasks_tot

    # Create list of arguments
    components = list(range(n_ftasks_tot))

    # Divide arguments in batches of size n_tasks_per_script
    batches = []
    batch_size = n_ftasks_per_script
    for ind_chunk in range(0, len(components), batch_size):
        batches.append(components[ind_chunk : ind_chunk + batch_size])  # noqa
    assert len(batches) == math.ceil(n_ftasks_tot / n_ftasks_per_script)

    # Define python task
    task_path = str(tmp777_path / "fake_task_for_timing.py")
    shutil.copy(str(testdata_path / "fake_task_for_timing.py"), task_path)
    command = f"/usr/bin/python3 {task_path}"
    sleep_time = 1.0
    debug(sleep_time)

    # Construct all sbatch scripts and corresponding folders
    sbatch_scripts = []
    logdirs = []
    for ind_batch, batch in enumerate(batches):
        debug(batch)
        sbatch_script_path = tmp777_path / f"submit_batch_{ind_batch}.sbatch"
        logdir = str(tmp777_path / f"logs_batch_{ind_batch}")
        umask = os.umask(0)
        os.mkdir(logdir, 0o777)
        _ = os.umask(umask)
        debug(sbatch_script_path)
        debug(command)
        with sbatch_script_path.open("w") as f:
            sbatch_script = write_script(
                list_args=batch,
                num_tasks_max_running=n_parallel_ftasks_per_script,
                mem_per_task_MB=100,
                cpu_per_task=cpu_per_task,
                logdir=logdir,
                command=command,
                sleep_time=sleep_time,
            )
            debug(sbatch_script)
            f.write(sbatch_script)
        sbatch_script_path.chmod(0o777)
        sbatch_script_path = str(sbatch_script_path)
        sbatch_scripts.append(sbatch_script_path)
        logdirs.append(logdir)

    # Submit all sbatch scripts
    for sbatch_script_path in sbatch_scripts:
        res = subprocess.run(
            shlex.split(
                f"sudo -u {monkey_slurm_user} sbatch {sbatch_script_path}"
            ),
            capture_output=True,
            encoding="utf-8",
        )
        debug(res.stdout)
        debug(res.stderr)
        assert res.returncode == 0

    # Wait for execution of all jobs
    while True:
        squeue_list = run_squeue(
            header=True,
            squeue_format="%.8i %.9P %.14j %.8T   %.7c %.4C   %.10M %.12l   %.6D  %R",  # noqa
        )
        print(squeue_list)
        # Break while loop when squeue output only includes the header
        non_empty_lines = [line for line in squeue_list.split("\n") if line]
        if len(non_empty_lines) == 1:
            break
        time.sleep(1)

    # Find out total runtime
    for ind_batch, logdir in enumerate(logdirs):
        start_times = []
        end_times = []
        for res_file in glob.glob(f"{logdir}/results_*.json"):
            debug(res_file)
            with open(res_file, "r") as f:
                results = json.load(f)
                start_times.append(results["start_time_sec"])
                end_times.append(results["end_time_sec"])
        assert start_times
        assert end_times

        # Compute expected total runtime factor for given slurm job
        debug(len(batches[ind_batch]))
        debug(n_parallel_ftasks_per_script)
        runtime_factor = math.ceil(
            len(batches[ind_batch]) / n_parallel_ftasks_per_script
        )
        debug(runtime_factor)
        earliest_start_time = min(start_times)
        latest_end_time = max(end_times)
        total_runtime = latest_end_time - earliest_start_time
        expected_runtime = sleep_time * runtime_factor
        debug(total_runtime)
        debug(expected_runtime)
        assert math.isclose(
            total_runtime,
            expected_runtime,
            rel_tol=0.1,
            abs_tol=0.1,
        )
