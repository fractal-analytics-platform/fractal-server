import os
import shlex
import shutil
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
            f"--mem=$MEM_PER_TASK {command} {ind} 2 {logdir} &\n"
        )
    script += "wait\n"
    return script


N = 8


@pytest.mark.parametrize("max_tasks", [1, 2, 4, 8])
def test_slurm_script(
    max_tasks,
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
    testdata_path,
):

    # NOTE: by now we make everything 777

    sbatch_script_path = tmp777_path / "submit.sbatch"
    logdir = str(tmp777_path / "logs")
    umask = os.umask(0)
    os.mkdir(logdir, 0o777)
    _ = os.umask(umask)
    old_task_path = str(testdata_path / "fake_task_for_timing.py")
    task_path = str(tmp777_path / "fake_task_for_timing.py")
    shutil.copy(old_task_path, task_path)
    command = f"/usr/bin/python3 {task_path}"
    debug(sbatch_script_path)
    debug(command)

    with sbatch_script_path.open("w") as f:
        sbatch_script = write_script(
            num_tasks_tot=N,
            num_tasks_max_running=max_tasks,
            mem_per_task_MB=2,
            cpu_per_task=1,
            logdir=logdir,
            command=command,
        )
        f.write(sbatch_script)
    sbatch_script_path.chmod(0o777)

    sbatch_script_path = str(sbatch_script_path)
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

    while True:
        squeue_list = run_squeue(header=False)
        debug(squeue_list)
        time.sleep(1)
        if not squeue_list:
            break
