from pathlib import Path

import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from fractal_server.runner.executors.slurm_sudo.runner import (
    SlurmSudoRunner,
)
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.container
async def test_submit_with_slurm_account_and_worker_init(
    db,
    tmp777_path: Path,
    history_mock_for_submit,
    monkey_slurm,
    valid_user_id,
    slurm_sudo_resource_profile_objects,
):
    """
    Test that SLURM account and `worker_init` are set in submission script.
    """

    SLURM_ACCOUNT = "something-random"
    COMMON_SCRIPT_LINES = ["export MYVAR1=VALUE1", "export MYVAR2=Value2"]

    resource, profile = slurm_sudo_resource_profile_objects[:]
    history_run_id, history_unit_id, wftask_id = history_mock_for_submit
    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
        slurm_account=SLURM_ACCOUNT,
        common_script_lines=COMMON_SCRIPT_LINES,
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
    for expected_line in COMMON_SCRIPT_LINES + [
        f"#SBATCH --account={SLURM_ACCOUNT}"
    ]:
        assert expected_line in script_lines
