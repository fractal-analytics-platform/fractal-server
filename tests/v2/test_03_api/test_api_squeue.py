import pytest
from sqlalchemy.orm import Session

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSH
from tests.fixtures_computational_settings import _add_resource_profile_to_db

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
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)
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
async def test_run_squeue_error(
    client,
    fractal_ssh: FractalSSH,
    slurm_ssh_resource_profile_db,
    MockCurrentUser,
    db_sync: Session,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)
    resource, profile = slurm_ssh_resource_profile_db[:]
    resource.host = "invalid.example.org"

    _add_resource_profile_to_db(
        res=resource,
        prof=profile,
        db_sync=db_sync,
    )

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/?scope=accounts")
        assert res.status_code == 422
        assert res.json()["detail"] == "Error executing squeue command."


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
        res = await client.get(f"{PREFIX}/job/squeue/?scope=user")
        assert res.status_code == 200
        res_normalized = " ".join(res.text.split())
        expected = "JOBID PARTITION NAME USER ACCOUNT STATE TIME NODES"
        assert expected in res_normalized


@pytest.mark.container
async def test_run_squeue_invalid_scope(
    client,
    slurm_sudo_resource_profile_db,
    MockCurrentUser,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SUDO)
    resource, profile = slurm_sudo_resource_profile_db[:]

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/?scope=foo")
        assert res.status_code == 422
        assert (
            res.json()["detail"][0]["msg"]
            == "Input should be 'all', 'user' or 'accounts'"
        )
