import pytest

from fractal_server.runner.executors.slurm_rest.job_queue_table import (
    format_job_queue_table,
)
from fractal_server.runner.executors.slurm_rest.job_queue_table import (
    format_seconds,
)

NOW = 1_794_401_237
HEADER = (
    "       JOBID PARTITION                     NAME           USER"
    "        ACCOUNT       STATE         TIME  NODES CPUS MIN_MEMORY"
    " NODELIST(REASON)"
)


def _n(number: int) -> dict:
    return dict(set=True, infinite=False, number=number)


def _job(job_id: int, **kwargs) -> dict:
    job = dict(
        job_id=job_id,
        partition="batchcpu",
        name="fractal-squeue-format-probe-with-a-long-name",
        user_name="alice",
        account="pcdefault",
        job_state=["PENDING"],
        start_time=_n(0),
        end_time=_n(0),
        node_count=_n(1),
        cpus=_n(1),
        memory_per_node=_n(100),
        memory_per_cpu=dict(set=False, infinite=False, number=0),
        nodes="",
        state_reason="JobHeldUser",
    )
    job.update(kwargs)
    return job


def test_format_job_queue_table():
    """
    Same columns as `squeue`; finished jobs are not shown; sorted by job ID.
    """
    start = NOW - 437243
    jobs = [
        _job(
            15177,
            name="import_bench",
            user_name="bob",
            job_state=["RUNNING"],
            start_time=_n(start),
            end_time=_n(start + 31536000),
            cpus=_n(2),
            memory_per_node=_n(4096),
            nodes="batchcpu-dy-x8i8xlarge-1",
        ),
        _job(15402, name="Projection", job_state=["COMPLETED"]),
        _job(
            15406,
            cpus=_n(2),
            memory_per_node=dict(set=False, infinite=False, number=0),
            memory_per_cpu=_n(1536),
        ),
        _job(15403),
    ]
    assert format_job_queue_table(jobs, now=NOW) == (
        f"{HEADER}\n"
        "       15177  batchcpu             import_bench            bob"
        "      pcdefault     RUNNING   5-01:27:23      1    2      4096M"
        " batchcpu-dy-x8i8xlarge-1\n"
        "       15403  batchcpu fractal-squeue-format-pr          alice"
        "      pcdefault     PENDING         0:00      1    1       100M"
        " (JobHeldUser)\n"
        "       15406  batchcpu fractal-squeue-format-pr          alice"
        "      pcdefault     PENDING         0:00      1    2      1536M"
        " (JobHeldUser)\n"
    )


def test_format_job_queue_table_empty():
    assert format_job_queue_table([], now=NOW) == f"{HEADER}\n"


def test_format_job_queue_table_completing():
    """
    A job whose epilog is still running is shown (as `COMPLETING`, with its
    nodes and run time until its end).
    """
    job = _job(
        1,
        job_state=["COMPLETED", "COMPLETING"],
        start_time=_n(NOW - 10),
        end_time=_n(NOW - 4),
        nodes="node-1",
    )
    fields = format_job_queue_table([job], now=NOW).splitlines()[1].split()
    assert fields[5:7] == ["COMPLETING", "0:06"]
    assert fields[-1] == "node-1"


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, "0:00"),
        (5, "0:05"),
        (65, "1:05"),
        (3723, "1:02:03"),
        (437243, "5-01:27:23"),
        (-1, "0:00"),
    ],
)
def test_format_seconds(seconds, expected):
    assert format_seconds(seconds) == expected
