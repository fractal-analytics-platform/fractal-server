import pytest
from sqlalchemy.orm import Session

from fractal_server.app.models import OAuthAccount
from fractal_server.app.routes.api.v2 import job as job_module
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.runner.executors.slurm_rest.settings import (
    SlurmRestSettings,
)
from fractal_server.runner.executors.slurm_rest.settings import (
    get_slurm_rest_settings,
)
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject
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
        slurm_accounts=["foo"],
    ) as _:
        res = await client.get(f"{PREFIX}/job/squeue/?scope=accounts")
        assert res.status_code == 422
        assert (
            res.json()["detail"]
            == "Error executing squeue command - please retry later."
        )


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


@pytest.mark.parametrize(
    "rest_enabled,n_sso_accounts,expected_user",
    [
        # SLURM REST runner: jobs run as the SSO user
        ("true", 1, "sso-user"),
        # No SSO account (no REST jobs): the SSH user
        ("true", 0, None),
        # SLURM/SSH runner: jobs run as the SSH user
        ("false", 1, None),
    ],
)
async def test_run_squeue_user_slurm_rest(
    rest_enabled: str,
    n_sso_accounts: int,
    expected_user: str | None,
    client,
    db,
    monkeypatch,
    slurm_ssh_resource_profile_fake_db,
    MockCurrentUser,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)
    _, profile = slurm_ssh_resource_profile_fake_db[:]
    expected_user = expected_user or profile.username
    commands = []

    class _FakeSSH:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def run_command(self, *, cmd: str) -> str:
            commands.append(cmd)
            return "JOBID"

    monkeypatch.setattr(job_module, "SingleUseFractalSSH", _FakeSSH)
    rest_settings = SlurmRestSettings(
        _env_file=None,
        FRACTAL_SLURM_REST_ENABLED=rest_enabled,
        FRACTAL_SLURM_REST_URL="https://slurmrestd.example.org",
        FRACTAL_SLURM_REST_CA_BUNDLE=None,
    )

    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        for _ in range(n_sso_accounts):
            db.add(
                OAuthAccount(
                    user_id=user.id,
                    oauth_name="openid",
                    access_token="x",
                    account_id="sso-user",
                    account_email="sso-user@example.org",
                )
            )
        await db.commit()

        Inject.override(get_slurm_rest_settings, lambda: rest_settings)
        try:
            res = await client.get(f"{PREFIX}/job/squeue/?scope=user")
        finally:
            Inject.pop(get_slurm_rest_settings)
    assert res.status_code == 200
    assert len(commands) == 1
    assert commands[0].startswith(f"squeue --user {expected_user} ")
