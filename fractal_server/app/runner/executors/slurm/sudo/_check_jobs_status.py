from subprocess import run  # nosec

from cfut.slurm import STATES_FINISHED

from ......logger import set_logger


logger = set_logger(__name__)


def run_squeue(job_ids):
    res = run(  # nosec
        [
            "squeue",
            "--noheader",
            "--format=%i %T",
            "--jobs",
            ",".join([str(j) for j in job_ids]),
            "--states=all",
        ],
        capture_output=True,
        encoding="utf-8",
        check=False,
    )
    if res.returncode != 0:
        logger.warning(
            f"squeue command with {job_ids}"
            f" failed with:\n{res.stderr=}\n{res.stdout=}"
        )

    return res


def _jobs_finished(job_ids) -> set[str]:
    """
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

    res = run_squeue(job_ids)
    if res.returncode == 0:
        id_to_state = {
            out.split()[0]: out.split()[1] for out in res.stdout.splitlines()
        }
    else:
        id_to_state = dict()
        for j in job_ids:
            res = run_squeue([j])
            if res.returncode != 0:
                logger.info(f"Job {j} not found. Marked it as completed")
                id_to_state.update({str(j): "COMPLETED"})
            else:
                id_to_state.update(
                    {res.stdout.split()[0]: res.stdout.split()[1]}
                )

    # Finished jobs only stay in squeue for a few mins (configurable). If
    # a job ID isn't there, we'll assume it's finished.
    return {
        j
        for j in job_ids
        if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
    }
