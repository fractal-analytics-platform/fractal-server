import os
import shlex
import subprocess
import time

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue


def write_script(
    num_tasks_tot: int,
    num_tasks_max_running: int,
    mem_per_task_MB: int,
    cpu_per_task: int,
    logdir: str,
    command: str,
):

    mem_tot_MB = mem_per_task_MB * num_tasks_max_running
    script = "\n".join(
        (
            "#!/bin/bash",
            "#SBATCH --partition=main",
            f"#SBATCH --err={logdir}/err",
            f"#SBATCH --out={logdir}/out",
            f"#SBATCH --cpus-per-task={cpu_per_task}",
            f"#SBATCH --mem={mem_tot_MB}MB",
            "",
            f"MEM_PER_TASK={mem_per_task_MB}MB",
            "",
            "",
        )
    )

    for ind in range(num_tasks_tot):
        script += (
            "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
            f"--mem=$MEM_PER_TASK {command} {ind} 5 &\n"
        )
    script += "wait\n"
    return script


@pytest.mark.skip
def test_slurm_script(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
    testdata_path,
):
    sbatch_script_path = str(tmp777_path / "submit.sbatch")
    logdir = str(tmp777_path / "logs")
    os.mkdir(logdir)
    task_path = str(testdata_path / "fake_task_for_timing.py")
    command = f"/usr/bin/python3 {task_path}"
    debug(sbatch_script_path)
    debug(command)

    with open(sbatch_script_path, "w") as f:
        sbatch_script = write_script(
            num_tasks_tot=1,
            num_tasks_max_running=1,
            mem_per_task_MB=1,
            cpu_per_task=1,
            logdir=logdir,
            command=command,
        )
        f.write(sbatch_script)

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

    return

    for ind in range(20):
        squeue = run_squeue()
        debug(squeue)
        time.sleep(1)
