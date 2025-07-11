import pytest

from fractal_server.app.runner.executors.slurm_sudo.runner import (
    SudoSlurmRunner,
)
from tests.fixtures_slurm import SLURM_USER


@pytest.mark.container
async def test_check_fractal_server_versions_executable(
    tmp777_path,
    monkey_slurm,
    current_py_version,
    override_settings_factory,
    monkeypatch,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )
    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
    ) as runner:
        # Successful check
        runner.check_fractal_server_versions()

        # Set up mock
        def patched_json_loads(*args, **kwargs):
            return dict(fractal_server="9.9.9")

        monkeypatch.setattr("json.loads", patched_json_loads)

        # Failed check
        with pytest.raises(RuntimeError, match="version mismatch"):
            runner.check_fractal_server_versions()
