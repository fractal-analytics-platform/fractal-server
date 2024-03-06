from subprocess import CalledProcessError  # nosec
from subprocess import PIPE  # nosec
from subprocess import run  # nosec

STATES_FINISHED = {  # https://slurm.schedmd.com/squeue.html#lbAG
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


def _custom_jobs_finished(job_ids, logger):
    """Check which ones of the given Slurm jobs already finished"""

    # If there is no Slurm job to check, return right away
    if not job_ids:
        return set()

    try:
        res = run(  # nosec
            [
                "squeue",
                "--noheader",
                "--format=%i %T",
                "--jobs",
                ",".join([str(j) for j in job_ids]),
                "--states=all",
            ],
            stdout=PIPE,
            stderr=PIPE,
            encoding="utf-8",
            check=True,
        )

        id_to_state = dict(
            [
                stdout.strip().partition(" ")[::2]
                for stdout in res.stdout.splitlines()
            ]
        )
    except CalledProcessError:
        id_to_state = dict()
        for j in job_ids:
            res = run(  # nosec
                [
                    "squeue",
                    "--noheader",
                    "--format=%i %T",
                    "--jobs",
                    str(j),
                    "--states=all",
                ],
                stdout=PIPE,
                stderr=PIPE,
                encoding="utf-8",
                check=False,
            )
            if res.returncode != 0:
                detail = run(  # nosec
                    ["sacct", "-j", str(j)],
                    stdout=PIPE,
                    stderr=PIPE,
                    encoding="utf-8",
                )
                logger.info(f"{detail}")

                id_to_state.update(j="COMPLETED")
            id_to_state.update(res.stdout.strip().partition(" ")[::2])

    # Finished jobs only stay in squeue for a few mins (configurable). If
    # a job ID isn't there, we'll assume it's finished.
    return {
        j
        for j in job_ids
        if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
    }
