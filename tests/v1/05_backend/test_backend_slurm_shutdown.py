import shlex
import subprocess
import sys
import time
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from tests.fixtures_slurm import run_squeue
from tests.fixtures_slurm import SLURM_USER

sys.path.append(Path(__file__).parent)
from test_backend_slurm import TestingFractalSlurmExecutor  # noqa: E402


def _check_no_running_jobs():
    res = subprocess.run(
        shlex.split("squeue --noheader --states=PD,CF,R "),
        capture_output=True,
        encoding="utf-8",
    )
    debug(res.stderr)
    debug(res.stdout)
    if res.returncode != 0:
        debug(res.stderr)
    assert res.returncode == 0
    assert res.stdout == ""


def test_direct_shutdown_during_submit(
    monkey_slurm,
    tmp777_path,
):
    """
    Test the FractalSlurmExecutor.shutdown method directly
    """

    # NOTE: this function has to be defined inside the test function, so that
    # this works correctly with cloudpickle. In principle one could make it
    # work with cloudpickle as well, via register_pickle_by_value (see
    # https://github.com/fractal-analytics-platform/fractal-server/issues/690)
    # but we observed some unexpected behavior and did not investigate further.
    # Note that this only affects the CI, while the function to be executed via
    # FractalSlurmExecutor in fractal-server are always imported from a kwown
    # package (i.e. fractal-server itself).
    def _sleep_and_return(sleep_time):
        time.sleep(sleep_time)
        return 42

    with TestingFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
        slurm_poll_interval=2,
        keep_pickle_files=True,
    ) as executor:

        fut = executor.submit(_sleep_and_return, 100)
        debug(fut)
        debug(run_squeue())

        debug("Now send shutdown")
        executor.shutdown()
        # executor.wait_thread.shutdown = True
        debug(fut)

        _check_no_running_jobs()

        try:
            _ = fut.result()
        except JobExecutionError as e:
            debug(e)
        except Exception as e:
            debug(e)
            raise e


def test_indirect_shutdown_during_submit(
    monkey_slurm,
    tmp777_path,
    tmp_path,
):
    """
    Test the FractalSlurmExecutor.shutdown method indirectly, that is, when it
    is triggered by the presence of a shutdown_file
    """
    shutdown_file = tmp_path / "shutdown"

    executor = TestingFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
        slurm_poll_interval=1,
        keep_pickle_files=True,
        shutdown_file=str(shutdown_file),
    )

    # NOTE: this has to be defined here, see note above and
    # https://github.com/fractal-analytics-platform/fractal-server/issues/690
    def _sleep_and_return(sleep_time):
        time.sleep(sleep_time)
        return 42

    res = executor.submit(_sleep_and_return, 100)
    debug(res)
    debug(run_squeue())

    with shutdown_file.open("w") as f:
        f.write("")
    assert shutdown_file.exists()
    time.sleep(2)

    debug(executor.wait_thread.shutdown)
    assert executor.wait_thread.shutdown

    time.sleep(4)

    debug(res)
    debug(run_squeue())

    _check_no_running_jobs()

    try:
        _ = res.result()
    except JobExecutionError as e:
        debug(e)
    except Exception as e:
        debug(e)
        raise e


def test_indirect_shutdown_during_map(
    monkey_slurm,
    tmp777_path,
    tmp_path,
):
    def fun_sleep(dummy):
        time.sleep(100)
        return 42

    shutdown_file = tmp_path / "shutdown"

    # NOTE: the executor.map call below is blocking. For this reason, we write
    # the shutdown file from a subprocess.Popen, so that we can make it happen
    # during the execution.
    shutdown_sleep_time = 2
    tmp_script = (tmp_path / "script.sh").as_posix()
    debug(tmp_script)
    with open(tmp_script, "w") as f:
        f.write(f"sleep {shutdown_sleep_time}\n")
        f.write(f"cat NOTHING > {shutdown_file.as_posix()}\n")

    tmp_stdout = open((tmp_path / "stdout").as_posix(), "w")
    tmp_stderr = open((tmp_path / "stderr").as_posix(), "w")
    subprocess.Popen(
        shlex.split(f"bash {tmp_script}"),
        stdout=tmp_stdout,
        stderr=tmp_stderr,
    )

    with TestingFractalSlurmExecutor(
        slurm_user=SLURM_USER,
        workflow_dir_local=tmp777_path,
        workflow_dir_remote=tmp777_path,
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

    tmp_stdout.close()
    tmp_stderr.close()
