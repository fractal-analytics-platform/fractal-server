from fractal_server.app.runner.executors.slurm_common._job_states import (
    STATES_FINISHED,
)
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH

logger = set_logger(__name__)


def run_squeue(
    *,
    job_ids: list[str],
    fractal_ssh: FractalSSH,
) -> str:
    job_id_single_str = ",".join([str(j) for j in job_ids])
    cmd = (
        f"squeue --noheader --format='%i %T' --jobs {job_id_single_str}"
        " --states=all"
    )
    stdout = fractal_ssh.run_command(cmd)
    return stdout


def get_finished_jobs_ssh(
    *,
    fractal_ssh: FractalSSH,
    job_ids: list[str],
) -> set[str]:
    """
    # FIXME: make uniform with non-ssh one

    Check which ones of the given Slurm jobs already finished

    The function is based on the `_jobs_finished` function from
    clusterfutures (version 0.5).
    Original Copyright: 2022 Adrian Sampson
    (released under the MIT licence)
    """

    # If there is no Slurm job to check, return right away
    if not job_ids:
        return set()

    id_to_state = dict()

    try:
        stdout = run_squeue(job_ids=job_ids, fractal_ssh=fractal_ssh)
        id_to_state = {
            line.split()[0]: line.split()[1] for line in stdout.splitlines()
        }
    except Exception:  # FIXME
        id_to_state = dict()
        for j in job_ids:
            try:
                stdout = run_squeue([j])
                id_to_state.update({stdout.split()[0]: stdout.split()[1]})
            except Exception:
                logger.info(f"Job {j} not found. Marked it as completed")
                id_to_state.update({str(j): "COMPLETED"})

    # Finished jobs only stay in squeue for a few mins (configurable). If
    # a job ID isn't there, we'll assume it's finished.
    return {
        j
        for j in job_ids
        if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
    }
