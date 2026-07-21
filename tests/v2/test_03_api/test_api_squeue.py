import pytest

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSH

PREFIX = "/api/v2"


async def test_run_squeue_not_available_local(
    client,
    override_settings_factory,
    local_resource_profile_db,
    MockCurrentUser,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.LOCAL)
    _, profile = local_resource_profile_db[:]

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/")
        assert res.status_code == 422
        assert (
            res.json()["detail"] == "This endpoint is not available for "
            "FRACTAL_RUNNER_BACKEND=local."
        )


@pytest.mark.ssh
@pytest.mark.container
async def test_run_squeue_ssh_success(
    client,
    fractal_ssh: FractalSSH,
    slurm_ssh_resource_profile_db,
    MockCurrentUser,
):
    fractal_ssh.default_lock_timeout = 1.0
    resource, profile = slurm_ssh_resource_profile_db[:]

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/")
        assert res.status_code == 200
        res_normalized = " ".join(res.text.split())
        expected = "JOBID PARTITION NAME USER ACCOUNT STATE TIME NODES"
        assert expected in res_normalized


@pytest.mark.ssh
@pytest.mark.container
async def test_run_squeue_ssh_connection_error(
    client, slurm_ssh_resource_profile_db, MockCurrentUser
):
    _, profile = slurm_ssh_resource_profile_db[:]

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/")
        assert res.status_code == 503
        assert res.json()["detail"] == "Cannot establish SSH connection."


@pytest.mark.container
async def test_run_squeue_sudo_success(
    client,
    monkey_slurm,
    override_settings_factory,
    slurm_sudo_resource_profile_db,
    MockCurrentUser,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SUDO)
    resource, profile = slurm_sudo_resource_profile_db[:]

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/")
        assert res.status_code == 200
        res_normalized = " ".join(res.text.split())
        expected = "JOBID PARTITION NAME USER ACCOUNT STATE TIME NODES"
        assert expected in res_normalized
