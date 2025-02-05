from fastapi import FastAPI

from fractal_server.main import lifespan
from fractal_server.ssh._fabric import FractalSSHList


async def test_lifespan_slurm_ssh(
    override_settings_factory,
    slurmlogin_ip,
    ssh_keys: dict[str, str],
    tmp777_path,
    testdata_path,
    db,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_SLURM_WORKER_PYTHON="/not/relevant",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )
    app = FastAPI()
    async with lifespan(app):
        assert len(app.state.jobsV2) == 0
        assert isinstance(app.state.fractal_ssh_list, FractalSSHList)
        assert app.state.fractal_ssh_list.size == 0
