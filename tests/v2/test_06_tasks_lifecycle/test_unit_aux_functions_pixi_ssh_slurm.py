import logging

import pytest

from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    _log_change_of_job_state,
)
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import _run_squeue
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    _verify_success_file_exists,
)
from fractal_server.tasks.v2.ssh._pixi_slurm_ssh import (
    FRACTAL_SQUEUE_ERROR_STATE,
)


def test_log_change_of_job_state(caplog):
    LOGGER_NAME = "my-logger"
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    _log_change_of_job_state(
        old_state=None,
        new_state="new_state",
        logger_name=LOGGER_NAME,
    )
    assert "state changed" in caplog.text


def test_run_squeue_failure():
    state = _run_squeue(
        fractal_ssh=FractalSSH(connection=None),
        squeue_cmd="fake",
        logger_name="my-logger",
    )
    assert state == FRACTAL_SQUEUE_ERROR_STATE


@pytest.mark.container
@pytest.mark.ssh
def test_verify_success_file_exists(fractal_ssh: FractalSSH):
    with pytest.raises(RuntimeError, match="missing"):
        _verify_success_file_exists(
            fractal_ssh=fractal_ssh,
            success_file_remote="/missing",
            logger_name="my-logger",
        )
