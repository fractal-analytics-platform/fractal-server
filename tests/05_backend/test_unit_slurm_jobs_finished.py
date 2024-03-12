from fractal_server.app.runner._slurm._check_jobs_status import (
    _custom_jobs_finished,
)


def test_jobs_finished(monkey_slurm):

    jobs_finished = _custom_jobs_finished([1, 2, 4])
    assert jobs_finished == {1, 2, 4}

    jobs_finished = _custom_jobs_finished([1])
    assert jobs_finished == {1}

    jobs_finished = _custom_jobs_finished(["a"])
    assert jobs_finished == {"a"}
