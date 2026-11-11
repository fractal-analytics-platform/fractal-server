import pytest
from sqlalchemy.orm import Session

from fractal_server.app.routes.api.v2 import job as job_module
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.runner.executors.slurm_rest import (
    job_queue_table as job_queue_table_module,
)
from fractal_server.runner.executors.slurm_rest.client import SlurmRestError
from fractal_server.runner.executors.slurm_rest.token import SlurmTokenError
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


def _rest_job(job_id: int, user_name: str, account: str) -> dict:
    return dict(
        job_id=job_id,
        partition="main",
        name=f"job-{job_id}",
        user_name=user_name,
        account=account,
        job_state=["PENDING"],
        state_reason="Priority",
        node_count=dict(set=True, infinite=False, number=1),
        cpus=dict(set=True, infinite=False, number=1),
        memory_per_node=dict(set=True, infinite=False, number=100),
    )


@pytest.mark.parametrize(
    "scope,slurm_accounts,expected_job_ids",
    [
        ("all", [], [1, 2, 3]),
        # Jobs of the SLURM user of the token, not of `profile.username`
        ("user", [], [1, 3]),
        ("accounts", ["acc-b"], [2, 3]),
        # No SLURM accounts: no filter, as with `squeue`
        ("accounts", [], [1, 2, 3]),
    ],
)
async def test_run_squeue_slurm_rest(
    scope: str,
    slurm_accounts: list[str],
    expected_job_ids: list[int],
    client,
    monkeypatch,
    slurm_rest_resource_profile_fake_db,
    MockCurrentUser,
    override_settings_factory,
):
    """
    On `slurm_rest` resources, `squeue`-like output from `GET /jobs` with the
    user's token (no SSH).
    """
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_REST)
    resource, profile = slurm_rest_resource_profile_fake_db[:]
    client_kwargs = {}

    class _FakeTokenProvider:
        slurm_username = "alice"

        def get_token(self, force_refresh: bool = False) -> str:
            return "token"

    class _FakeClient:
        def __init__(self, **kwargs):
            client_kwargs.update(kwargs)

        def list_jobs(self) -> list[dict]:
            return [
                _rest_job(1, "alice", "acc-a"),
                _rest_job(2, "bob", "acc-b"),
                _rest_job(3, "alice", "acc-b"),
            ]

    def _no_ssh(**kwargs):
        raise AssertionError("SSH must not be used.")

    monkeypatch.setattr(job_module, "SingleUseFractalSSH", _no_ssh)
    monkeypatch.setattr(
        job_queue_table_module,
        "get_token_provider",
        lambda **_: _FakeTokenProvider(),
    )
    monkeypatch.setattr(job_queue_table_module, "SlurmRestClient", _FakeClient)

    async with MockCurrentUser(
        is_verified=True,
        profile_id=profile.id,
        slurm_accounts=slurm_accounts,
    ):
        res = await client.get(f"{PREFIX}/job/squeue/?scope={scope}")
    assert res.status_code == 200
    lines = res.text.splitlines()
    assert lines[0].split() == [
        "JOBID",
        "PARTITION",
        "NAME",
        "USER",
        "ACCOUNT",
        "STATE",
        "TIME",
        "NODES",
        "CPUS",
        "MIN_MEMORY",
        "NODELIST(REASON)",
    ]
    assert [int(line.split()[0]) for line in lines[1:]] == expected_job_ids
    rest_config = resource.jobs_runner_config["rest"]
    assert client_kwargs["base_url"] == rest_config["url"]
    assert client_kwargs["api_version"] == rest_config["api_version"]
    assert client_kwargs["username"] == "alice"


@pytest.mark.parametrize(
    "error,expected_detail",
    [
        (
            SlurmTokenError("No SSO account for user 1. Hint: log in."),
            "No SSO account for user 1. Hint: log in.",
        ),
        (
            SlurmRestError("GET /jobs -> HTTP 502: []", status=502),
            "Error querying SLURM jobs - please retry later.",
        ),
    ],
)
async def test_run_squeue_slurm_rest_error(
    error: Exception,
    expected_detail: str,
    client,
    monkeypatch,
    slurm_rest_resource_profile_fake_db,
    MockCurrentUser,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_REST)
    _, profile = slurm_rest_resource_profile_fake_db[:]

    def _fail(**kwargs):
        raise error

    monkeypatch.setattr(job_module, "get_job_queue_table", _fail)
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        res = await client.get(f"{PREFIX}/job/squeue/")
    assert res.status_code == 422
    assert res.json()["detail"] == expected_detail
