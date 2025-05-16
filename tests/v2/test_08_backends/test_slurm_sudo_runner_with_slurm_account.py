from pathlib import Path

from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    SudoSlurmRunner,
)
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


async def test_submit_with_slurm_account(
    db,
    tmp777_path: Path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
):
    """
    Test that SLURM account is written in submission script.
    """

    SLURM_ACCOUNT = "something-random"

    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
        slurm_account=SLURM_ACCOUNT,
    ) as runner:
        result, exception = runner.submit(
            base_command="true",
            workflow_task_order=0,
            workflow_task_id=wftask_id,
            task_name="fake-task-name",
            parameters=dict(zarr_urls=ZARR_URLS),
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
            user_id=valid_user_id,
        )
    # Verify that submission script includes `account` option
    submission_script = next((tmp777_path / "server").glob("*/*.sh"))
    debug(submission_script)
    with submission_script.open() as f:
        script_lines = f.read().strip().split("\n")
        expected_line = f"#SBATCH --account={SLURM_ACCOUNT}"
        assert expected_line in script_lines
