import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    SudoSlurmRunner,
)
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.container
async def test_executor_error(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
):
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit

    class SudoSlurmRunnerMod(SudoSlurmRunner):
        # Inject a failing command in the SLURM submission script
        def _prepare_single_slurm_job(self, *args, **kwargs) -> str:
            submit_command = super()._prepare_single_slurm_job(*args, **kwargs)
            script_path = submit_command.split(" ")[-1]
            with open(script_path) as f:
                script_contents = f.read()
            script_contents = script_contents.replace(
                "pwd", "ls --fake-option"
            )
            with open(script_path, "w") as f:
                f.write(script_contents)
            return submit_command

    with SudoSlurmRunnerMod(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters={},
            task_files=get_dummy_task_files(
                tmp777_path,
                component="0",
                is_slurm=True,
            ),
            task_type="converter_non_parallel",
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )

        debug(runner.executor_error_log)
        assert runner.executor_error_log is not None
