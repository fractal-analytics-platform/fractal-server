import shlex
import subprocess
import time

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
            f"--mem=$MEM_PER_TASK {command} {ind} 1 &\n"
        )
    script += "wait\n"
    return script


def test_slurm_script(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    script_path = tmp777_path / "submit.sbatch"
    print(script_path)
    with script_path.open("w") as f:
        sbatch_script = write_script(
            num_tasks_tot=1,
            num_tasks_max_running=1,
            mem_per_task_MB=1,
            cpu_per_task=1,
            logdir=str(tmp777_path),
            command="sleep",
        )
        f.write(sbatch_script)

    res = subprocess.run(
        shlex.split(f"sudo -u {monkey_slurm_user} sbatch {str(script_path)}"),
        capture_output=True,
        encoding="utf-8",
    )
    debug(res.stdout)
    debug(res.stderr)
    assert res.returncode == 0

    while True:
        squeue = run_squeue()
        debug(squeue)
        time.sleep(1)
