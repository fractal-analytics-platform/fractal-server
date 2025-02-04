import json
from pathlib import Path

import pytest

from fractal_server.app.runner.executors.slurm.sudo.executor import (
    FractalSlurmExecutor,
)


def test_check_remote_runner_python_interpreter(
    monkeypatch, override_settings_factory
):
    remote_version = "1.0.0"
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/remote/python")

    def mock_subprocess_run_or_raise(cmd):
        class MockCompletedProcess(object):
            stdout: str = json.dumps({"fractal_server": remote_version})

        return MockCompletedProcess()

    with pytest.raises(
        RuntimeError, match="No such file or directory: '/remote/python'"
    ):
        FractalSlurmExecutor(
            slurm_user="test_user",
            workflow_dir_local=Path("/local/workflow"),
            workflow_dir_remote=Path("/remote/workflow"),
        )

    monkeypatch.setattr(
        (
            "fractal_server.app.runner.executors.slurm.sudo.executor"
            "._subprocess_run_or_raise"
        ),
        mock_subprocess_run_or_raise,
    )

    with pytest.raises(RuntimeError, match="Fractal-server version mismatch"):
        FractalSlurmExecutor(
            slurm_user="test_user",
            workflow_dir_local=Path("/local/workflow"),
            workflow_dir_remote=Path("/remote/workflow"),
        )
