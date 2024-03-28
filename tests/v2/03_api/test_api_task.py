from devtools import debug

from fractal_server.app.schemas.v2 import TaskCreateV2

PREFIX = "/api/v2/task"


async def test_non_verified_user(client, MockCurrentUser):
    """
    Test that a non-verified user is not authorized to make POST/PATCH task
    cals.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(f"{PREFIX}/", json={})
        assert res.status_code == 401

        res = await client.patch(f"{PREFIX}/123/", json={})
        assert res.status_code == 401


async def test_task_get_list(db, client, task_factory_v2, MockCurrentUser):
    await task_factory_v2(index=1)
    await task_factory_v2(index=2)
    t = await task_factory_v2(
        index=3,
        args_schema_non_parallel=dict(a=1),
        args_schema_parallel=dict(b=2),
    )

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        assert len(data) == 3
        assert data[2]["id"] == t.id
        assert data[2]["args_schema_non_parallel"] == dict(a=1)
        assert data[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_non_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] == dict(a=1)
        assert res.json()[2]["args_schema_parallel"] is None

        res = await client.get(
            f"{PREFIX}/"
            "?args_schema_parallel=false&args_schema_non_parallel=False"
        )
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] is None


async def test_post_task(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        TASK_OWNER = user.username or user.slurm_user
        TASK_SOURCE = "some_source"

        # Successful task creations
        task = TaskCreateV2(
            name="task_name",
            # Compound
            source=f"{TASK_SOURCE}-compound",
            command_parallel="task_command_parallel",
            command_non_parallel="task_command_non_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["type"] == "compound"
        assert res.json()["owner"] == TASK_OWNER
        assert res.json()["source"] == f"{TASK_OWNER}:{TASK_SOURCE}-compound"
        task = TaskCreateV2(
            name="task_name",
            # Parallel
            source=f"{TASK_SOURCE}-parallel",
            command_parallel="task_command_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["type"] == "parallel"
        task = TaskCreateV2(
            name="task_name",
            # Non Parallel
            source=f"{TASK_SOURCE}-non_parallel",
            command_non_parallel="task_command_non_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["type"] == "non_parallel"

        # Fail for repeated task.source
        task = TaskCreateV2(
            name="new_task_name",
            source=f"{TASK_SOURCE}-compound",
            command_parallel="new_task_command",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 422

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422

    # Test multiple combinations of (username, slurm_user)
    SLURM_USER = "some_slurm_user"
    USERNAME = "some_username"
    # Case 1: (username, slurm_user) = (None, None)
    user_kwargs = dict(username=None, slurm_user=None, is_verified=True)
    payload = dict(name="task", command_parallel="cmd")
    payload["source"] = "source_x"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot add a new task because current user does not have "
            "`username` or `slurm_user` attributes."
        )
    # Case 2: (username, slurm_user) = (not None, not None)
    user_kwargs = dict(
        username=USERNAME, slurm_user=SLURM_USER, is_verified=True
    )
    payload["source"] = "source_y"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME
    # Case 3: (username, slurm_user) = (None, not None)
    user_kwargs = dict(username=None, slurm_user=SLURM_USER, is_verified=True)
    payload["source"] = "source_z"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == SLURM_USER
    # Case 4: (username, slurm_user) = (not None, None)
    user_kwargs = dict(username=USERNAME, slurm_user=None, is_verified=True)
    payload["source"] = "source_xyz"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME
