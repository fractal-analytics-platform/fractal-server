import os
import time
from pathlib import Path

from fractal_server.config import get_settings
from fractal_server.config import PixiSLURMConfig
from fractal_server.logger import get_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject


# https://slurm.schedmd.com/squeue.html#lbAG
STATES_FINISHED = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}


def run_script_on_remote_slurm(
    *,
    script_path: str,
    slurm_config: PixiSLURMConfig,
    fractal_ssh: FractalSSH,
    logger_name: str,
    prefix: str,
):
    """
    FIXME


    NOTE: This is called from within a try/except, thus we can use exceptions
    as a mechanism to propagate failure/errors.
    """

    logger = get_logger(logger_name=logger_name)
    settings = Inject(get_settings)

    # (1) Prepare remote submission script
    workdir_remote = Path(script_path).parent.as_posix()
    submission_script_remote = os.path.join(
        workdir_remote, f"{prefix}-submit.sh"
    )
    stderr_remote = os.path.join(workdir_remote, f"{prefix}-err.txt")
    stdout_remote = os.path.join(workdir_remote, f"{prefix}-out.txt")
    success_file_remote = os.path.join(workdir_remote, f"{prefix}-success.txt")
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --partition={slurm_config.partition}",
        f"#SBATCH --cpus-per-task={slurm_config.cpus}",
        f"#SBATCH --mem={slurm_config.mem}",
        f"#SBATCH --time={slurm_config.time}",
        f"#SBATCH --err={stderr_remote}",
        f"#SBATCH --out={stdout_remote}",
        f"#SBATCH -D {workdir_remote}",
        "",
        f"bash {script_path}",
        f"touch {success_file_remote}",
        "",
    ]
    script_contents = "\n".join(script_lines)
    fractal_ssh.write_remote_file(
        path=submission_script_remote,
        content=script_contents,
    )

    # (2) Submit SLURM job
    sbatch_cmd = f"sbatch --parsable {submission_script_remote} "
    try:
        stdout = fractal_ssh.run_command(cmd=sbatch_cmd)
    except Exception as e:
        logger.error(
            f"Submission of {submission_script_remote} failed. "
            f"Original error: {str(e)}"
        )
        raise e
    logger.debug(f"Now submit job {submission_script_remote} to SLURM.")
    job_id = int(stdout)
    logger.debug(f"SLURM-job submission successful ({job_id=}).")

    # (3) Monitor job
    squeue_cmd = (
        f"squeue --noheader --format='%i %T' --states=all --jobs={job_id}"
    )
    while True:
        try:
            stdout = fractal_ssh.run_command(cmd=squeue_cmd)
        except Exception as e:
            # FIXME: review this logic
            logger.info(
                f"`squeue` command failed (original error: {e}), "
                "consider the job as complete."
            )
            break
        state = stdout.strip().split()[1]
        logger.debug(f"Status of SLURM job {job_id}: {state}")
        if state in STATES_FINISHED:
            logger.debug(f"Exit retrieval loop ({state=}).")
            break
        time.sleep(settings.FRACTAL_SLURM_POLL_INTERVAL)

    if fractal_ssh.remote_exists(path=success_file_remote):
        logger.info(f"{success_file_remote=} exists.")
    else:
        raise RuntimeError(
            "SLURM job did not complete correctly "
            f"({success_file_remote=} missing)."
        )
