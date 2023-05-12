import asyncio
import logging
import threading
import time
from pathlib import Path

import pytest
from devtools import debug

from .fixtures_slurm import run_squeue
from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor
from fractal_server.app.runner.common import JobExecutionError


def _sleep_and_return(dummy):
    time.sleep(10)
    return 42


def test_direct_shutdown_during_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
):
    """
    Test the FractalSlurmExecutor.shutdown method directly
    """

    executor = FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    )

    res = executor.submit(_sleep_and_return, 100)
    debug(res)
    debug(run_squeue())
    executor.shutdown()
    debug(res)
    assert not run_squeue(header=False)
    with pytest.raises(JobExecutionError) as e:
        _ = res.result()
    debug(e)


def test_indirect_shutdown_during_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    tmp_path,
    cfut_jobs_finished,
):
    """
    Test the FractalSlurmExecutor.shutdown method indirectly, that is, when it
    is triggered by the presence of a shutdown_file
    """
    shutdown_file = tmp_path / "shutdown"

    executor = FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
        shutdown_file=str(shutdown_file),
    )

    res = executor.submit(_sleep_and_return, 100)
    debug(res)
    debug(run_squeue())

    with shutdown_file.open("w") as f:
        f.write("")
    assert shutdown_file.exists()

    time.sleep(5)

    debug(res)
    debug(run_squeue())

    assert not run_squeue(header=False)
    with pytest.raises(JobExecutionError):
        _ = res.result()


async def _write_shutdown_file(shutdown_file: Path, sleep_time):
    # The _auxiliary_scancel and _auxiliary_run functions are used as in
    # https://stackoverflow.com/a/59645689/19085332
    logging.warning(f"[_write_shutdown_file] run START {time.perf_counter()=}")
    # Wait `scancel_sleep_time` seconds, to let the SLURM job pass from PENDING
    # to RUNNING
    time.sleep(sleep_time)

    debug(run_squeue())
    # Scancel all jobs of the current SLURM user
    logging.warning(f"[_write_shutdown_file] run WRITE {time.perf_counter()=}")
    # Trigger shutdown
    with shutdown_file.open("w") as f:
        f.write("")
    assert shutdown_file.exists()
    logging.warning(f"[_write_shutdown_file] run END {time.perf_counter()=}")


def _auxiliary_run(shutdown_file: Path, sleep_time):
    # The _write_shutdown_file and _auxiliary_run functions are used as in
    # https://stackoverflow.com/a/59645689/19085332
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_write_shutdown_file(shutdown_file, sleep_time))
    loop.close()


def test_indirect_shutdown_during_map(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    tmp_path,
    cfut_jobs_finished,
):
    def fun_sleep(dummy):
        time.sleep(100)
        return 42

    shutdown_file = tmp_path / "shutdown"

    # NOTE: the executor.map call below is blocking. For this reason, we call
    # the scancel function from a different thread, so that we can make it
    # happen during the workflow execution The following block is based on
    # https://stackoverflow.com/a/59645689/19085332

    shutdown_sleep_time = 2
    logging.warning(f"PRE THREAD START {time.perf_counter()=}")
    _thread = threading.Thread(
        target=_auxiliary_run, args=(shutdown_file, shutdown_sleep_time)
    )
    _thread.start()
    logging.warning(f"POST THREAD START {time.perf_counter()=}")

    with FractalSlurmExecutor(
        slurm_user=monkey_slurm_user,
        working_dir=tmp777_path,
        working_dir_user=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
        shutdown_file=str(shutdown_file),
    ) as executor:

        res = executor.map(fun_sleep, range(25))
        debug(run_squeue())

        time.sleep(shutdown_sleep_time + 1)
        debug(run_squeue())

        with pytest.raises(JobExecutionError) as e:
            list(res)
        debug(e.value)
        assert "shutdown" in str(e.value)
