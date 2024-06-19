from fractal_server.app.runner.executors.slurm.sudo._check_jobs_status import (
    _jobs_finished,
)
from fractal_server.app.runner.executors.slurm.sudo._check_jobs_status import (
    run_squeue,
)


def test_run_squeue_non_patched(monkey_slurm):
    """
    This test runs the original run_squeue function, rather than
    patched_run_squeue. For this reason (and due to what is discussed in
    https://github.com/sampsyo/clusterfutures/pull/19) the squeue command
    fails, and the error is related to the command format.

    This behavior takes place because the squeue command (within the current
    CI setup) is run as part of a bash command (so that it can be prependend
    with docker commands). In this case, the whitespace is used for command
    splitting - this is why the patched_run_squeue fixture is needed.
    """
    res = run_squeue([1, 2, 3])
    assert res.returncode == 1
    assert res.stderr == "squeue: error: Invalid job id: %T\n"


def test_jobs_finished(monkey_slurm):

    jobs_finished = _jobs_finished([1, 2, 4])
    assert jobs_finished == {1, 2, 4}

    jobs_finished = _jobs_finished([1])
    assert jobs_finished == {1}

    jobs_finished = _jobs_finished(["a"])
    assert jobs_finished == {"a"}
