import os
import time
from pathlib import Path

from sqlalchemy.orm import Session

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.config import get_settings
from fractal_server.config import PixiSLURMConfig
from fractal_server.logger import get_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import get_current_log

FRACTAL_SQUEUE_ERROR_STATE = "__FRACTAL_SQUEUE_ERROR__"

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
    FRACTAL_SQUEUE_ERROR_STATE,
}


def _log_change_of_job_state(
    *,
    old_state: str | None,
    new_state: str,
    logger_name: str,
) -> None:
    """
    Emit a log for state changes.

    Args:
        old_state:
        new_state:
        logger_name:
    """
    if new_state != old_state:
        logger = get_logger(logger_name=logger_name)
        logger.debug(
            f"SLURM-job state changed from {old_state=} to {new_state=}."
        )


def _run_squeue(
    *,
    fractal_ssh: FractalSSH,
    squeue_cmd: str,
    logger_name: str,
) -> str:
    """
    Run a `squeue` command and handle exceptions.

    Args:
        fractal_ssh:
        logger_name:
        squeue_cmd:

    Return:
        state: The SLURM-job state.
    """
    try:
        stdout = fractal_ssh.run_command(cmd=squeue_cmd)
        state = stdout.strip().split()[1]
        return state
    except Exception as e:
        logger = get_logger(logger_name=logger_name)
        logger.info(f"`squeue` command failed (original error: {e})")
        return FRACTAL_SQUEUE_ERROR_STATE


def _verify_success_file_exists(
    *,
    fractal_ssh: FractalSSH,
    success_file_remote: str,
    logger_name: str,
) -> None:
    """
    Fail if the success sentinel file does not exist remotely.
    """
    if not fractal_ssh.remote_exists(path=success_file_remote):
        logger = get_logger(logger_name=logger_name)
        error_msg = f"{success_file_remote=} missing."
        logger.info(error_msg)
        raise RuntimeError(error_msg)


def run_script_on_remote_slurm(
    *,
    script_path: str,
    slurm_config: PixiSLURMConfig,
    fractal_ssh: FractalSSH,
    logger_name: str,
    log_file_path: Path,
    prefix: str,
    db: Session,
    activity: TaskGroupActivityV2,
):
    """
    Run a `pixi install` script as a SLURM job.

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
    logger.debug(f"Written {submission_script_remote=}.")

    activity.log = get_current_log(log_file_path)
    activity = add_commit_refresh(obj=activity, db=db)

    # (2) Submit SLURM job
    logger.debug("Now submit SLURM job.")
    sbatch_cmd = f"sbatch --parsable {submission_script_remote} "
    try:
        stdout = fractal_ssh.run_command(cmd=sbatch_cmd)
        job_id = int(stdout)
        logger.debug(f"SLURM-job submission successful ({job_id=}).")
    except Exception as e:
        logger.error(
            (
                f"Submission of {submission_script_remote} failed. "
                f"Original error: {str(e)}"
            )
        )
        raise e
    finally:
        activity.log = get_current_log(log_file_path)
        activity = add_commit_refresh(obj=activity, db=db)

    # (3) Monitor job
    squeue_cmd = (
        f"squeue --noheader --format='%i %T' --states=all --jobs={job_id}"
    )
    logger.debug(f"Start monitoring job with {squeue_cmd=}.")
    old_state = None
    while True:
        new_state = _run_squeue(
            fractal_ssh=fractal_ssh,
            squeue_cmd=squeue_cmd,
            logger_name=logger_name,
        )
        _log_change_of_job_state(
            old_state=old_state,
            new_state=new_state,
            logger_name=logger_name,
        )
        activity.log = get_current_log(log_file_path)
        activity = add_commit_refresh(obj=activity, db=db)
        if new_state in STATES_FINISHED:
            logger.debug(f"Exit retrieval loop (state={new_state}).")
            break
        old_state = new_state
        time.sleep(settings.FRACTAL_SLURM_POLL_INTERVAL)

    _verify_success_file_exists(
        fractal_ssh=fractal_ssh,
        logger_name=logger_name,
        success_file_remote=success_file_remote,
    )

    logger.info("SLURM-job execution completed successfully, continue.")
    activity.log = get_current_log(log_file_path)
    activity = add_commit_refresh(obj=activity, db=db)
