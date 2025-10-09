import os
import time
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.logger import get_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.config import PixiSLURMConfig
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


def _get_workdir_remote(script_paths: list[str]) -> str:
    """
    Check that there is one and only one `workdir`, and return it.

    Note: The `is_absolute` check is to filter out a `chmod` command.
    """
    workdirs = [
        Path(script_path).parent.as_posix()
        for script_path in script_paths
        if Path(script_path).is_absolute()
    ]
    if not len(set(workdirs)) == 1:
        raise ValueError(f"Invalid {script_paths=}.")
    return workdirs[0]


def _read_file_if_exists(
    *,
    fractal_ssh: FractalSSH,
    path: str,
) -> str:
    """
    Read a remote file if it exists, or return an empty string.
    """
    if fractal_ssh.remote_exists(path=path):
        return fractal_ssh.read_remote_text_file(path)
    else:
        return ""


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
        cmd_stdout = fractal_ssh.run_command(cmd=squeue_cmd)
        state = cmd_stdout.strip().split()[1]
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
    stderr_remote: str,
) -> None:
    """
    Fail if the success sentinel file does not exist remotely.

    Note: the `FractalSSH` methods in this function may fail, and such failures
    are not handled in this function. Any such failure, however, will lead to
    a "failed" task-group lifecycle activity (because it will raise an
    exception from within `run_script_on_remote_slurm`, which will then be
    handled at the calling-function level.
    """
    if not fractal_ssh.remote_exists(path=success_file_remote):
        logger = get_logger(logger_name=logger_name)
        error_msg = f"{success_file_remote=} missing."
        logger.info(error_msg)

        stderr = _read_file_if_exists(
            fractal_ssh=fractal_ssh, path=stderr_remote
        )
        if stderr:
            logger.info(f"SLURM-job stderr:\n{stderr}")
        raise RuntimeError(error_msg)


def run_script_on_remote_slurm(
    *,
    script_paths: list[str],
    slurm_config: dict[str, Any],
    fractal_ssh: FractalSSH,
    logger_name: str,
    log_file_path: Path,
    prefix: str,
    db: Session,
    activity: TaskGroupActivityV2,
    poll_interval: int,
):
    """
    Run a `pixi install` script as a SLURM job.

    NOTE: This is called from within a try/except, thus we can use exceptions
    as a mechanism to propagate failure/errors.
    """

    slurm_config_obj = PixiSLURMConfig(**slurm_config)

    logger = get_logger(logger_name=logger_name)

    # (1) Prepare remote submission script
    workdir_remote = _get_workdir_remote(script_paths)
    submission_script_remote = os.path.join(
        workdir_remote, f"{prefix}-submit.sh"
    )
    stderr_remote = os.path.join(workdir_remote, f"{prefix}-err.txt")
    stdout_remote = os.path.join(workdir_remote, f"{prefix}-out.txt")
    success_file_remote = os.path.join(workdir_remote, f"{prefix}-success.txt")
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --partition={slurm_config_obj.partition}",
        f"#SBATCH --cpus-per-task={slurm_config_obj.cpus}",
        f"#SBATCH --mem={slurm_config_obj.mem}",
        f"#SBATCH --time={slurm_config_obj.time}",
        f"#SBATCH --err={stderr_remote}",
        f"#SBATCH --out={stdout_remote}",
        f"#SBATCH -D {workdir_remote}",
        "",
    ]
    for script_path in script_paths:
        script_lines.append(f"bash {script_path}")
    script_lines.append(f"touch {success_file_remote}")

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
    sbatch_cmd = f"sbatch --parsable {submission_script_remote}"
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
        time.sleep(poll_interval)

    _verify_success_file_exists(
        fractal_ssh=fractal_ssh,
        logger_name=logger_name,
        success_file_remote=success_file_remote,
        stderr_remote=stderr_remote,
    )

    stdout = _read_file_if_exists(
        fractal_ssh=fractal_ssh,
        path=stdout_remote,
    )

    logger.info("SLURM-job execution completed successfully, continue.")
    activity.log = get_current_log(log_file_path)
    activity = add_commit_refresh(obj=activity, db=db)

    return stdout
