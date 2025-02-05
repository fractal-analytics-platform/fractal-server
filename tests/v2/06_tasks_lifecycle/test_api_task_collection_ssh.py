from fractal_server.ssh._fabric import FractalSSHList

PREFIX = "api/v2/task"


async def test_task_collection_ssh_failure_no_connection(
    db,
    app,
    client,
    MockCurrentUser,
    override_settings_factory,
    current_py_version: str,
):
    """
    Test exception handling for when SSH connection is not available.
    """

    # Assign empty FractalSSH object to app state
    app.state.fractal_ssh_list = FractalSSHList()

    # Override settings with Python/SSH configurations
    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    settings_overrides = {
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION": current_py_version,
        PY_KEY: f"/usr/bin/python{current_py_version}",
        "FRACTAL_RUNNER_BACKEND": "slurm_ssh",
    }
    override_settings_factory(**settings_overrides)

    user_settings_dict = dict(
        ssh_host="fake",
        ssh_username="fake",
        ssh_private_key_path="fake",
        ssh_tasks_dir="/fake",
        ssh_jobs_dir="/fake",
    )

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True),
        user_settings_dict=user_settings_dict,
    ):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 202
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()
        assert task_group_activity["status"] == "failed"
        assert "Cannot establish SSH connection" in task_group_activity["log"]
