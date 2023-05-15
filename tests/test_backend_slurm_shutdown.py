"""
"""
import asyncio
import importlib
import logging
import sys
import threading
import time
from pathlib import Path

import cloudpickle
import pytest
from devtools import debug

from ._auxiliary_functions import _sleep_and_return
from .fixtures_slurm import run_squeue
from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor
from fractal_server.app.runner.common import JobExecutionError


@pytest.fixture
def cloudpickle_import(testdata_path):
    """
    NOTE: the experimental `register_pickle_by_value` feature of cloudpickle>=2
    is described in
    https://github.com/cloudpipe/cloudpickle#overriding-pickles-serialization-mechanism-for-importable-constructs.

    Briefly:
    1. It allows us to unpickle a function even if we don't have access to the
       module where it was defined.
    2. It is experimental and can fail for some relevant cases (e.g. functions
       that include other imports, or a mix of by-value/by-reference pickling).

    For us, this feature could be quite useful as part of the CI, to avoid
    unexpected errors as in
    https://github.com/fractal-analytics-platform/fractal-server/issues/690.
    """

    # Import module by name/path
    module_name = "_auxiliary_functions"
    file_path = str(testdata_path.parent / "_auxiliary_functions.py")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # Configure by-value pickling for this module
    cloudpickle.register_pickle_by_value(module)

    yield

    # Clean up, after test
    cloudpickle.unregister_pickle_by_value(module)
    sys.modules.pop(module_name)


def test_direct_shutdown_during_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    cfut_jobs_finished,
    cloudpickle_import,
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
    executor.wait_thread.shutdown = True
    debug(res)
    assert not run_squeue(header=False)

    try:
        _ = res.result()
    except JobExecutionError as e:
        debug(e)
    except Exception as e:
        debug(e)
        raise e


def test_indirect_shutdown_during_submit(
    monkey_slurm,
    monkey_slurm_user,
    tmp777_path,
    tmp_path,
    cfut_jobs_finished,
    cloudpickle_import,
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
        slurm_poll_interval=1,
        keep_pickle_files=True,
        shutdown_file=str(shutdown_file),
    )

    res = executor.submit(_sleep_and_return, 100)
    debug(res)
    debug(run_squeue())

    with shutdown_file.open("w") as f:
        f.write("")
    assert shutdown_file.exists()
    time.sleep(1.5)

    debug(executor.wait_thread.shutdown)
    assert executor.wait_thread.shutdown

    time.sleep(2)

    debug(res)
    debug(run_squeue())

    assert not run_squeue(header=False)
    try:
        _ = res.result()
    except JobExecutionError as e:
        debug(e)
    except Exception as e:
        debug(e)
        raise e


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
