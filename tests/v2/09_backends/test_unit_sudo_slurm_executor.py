import json
from pathlib import Path

import pytest

from fractal_server.app.runner.executors.slurm.sudo.executor import (
    FractalSlurmExecutor,
)


def test_check_runner_node_python_interpreter(
    monkeypatch, override_settings_factory
):
    remote_version = "1.0.0"
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/remote/python")

    def mock_subprocess_run_or_raise(cmd):
        return type(
            "MockProcess",
            (),
            {"stdout": json.dumps({"fractal_server": remote_version})},
        )

    def mock_init(
        self,
        slurm_user,
        workflow_dir_local,
        workflow_dir_remote,
        *args,
        **kwargs
    ):
        self.slurm_user = slurm_user
        self.workflow_dir_local = workflow_dir_local
        self.workflow_dir_remote = workflow_dir_remote

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
