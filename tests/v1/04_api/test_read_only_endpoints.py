async def test_read_only_enpoints(
    client, override_settings_factory, MockCurrentUser
):

    override_settings_factory(FRACTAL_API_V1_MODE="include_read_only")

    GET = client.get

    v1_endpoints = [
        # Project
        (GET, "/api/v1/project/", None),
        (GET, "/api/v1/project/1/", None),
        # Task
        (GET, "/api/v1/task/", None),
        (GET, "/api/v1/task/1/", None),
        # Task Collection
        (GET, "/api/v1/task/collect/1/", None),
        # Dataset
        (GET, "/api/v1/project/1/dataset/", None),
        (GET, "/api/v1/project/1/dataset/1/", None),
        (GET, "/api/v1/project/1/dataset/1/resource/", None),
        (GET, "/api/v1/project/1/dataset/1/export_history/", None),
        (GET, "/api/v1/project/1/dataset/1/status/", None),
        (GET, "/api/v1/dataset/", None),
        # Workflow
        (GET, "/api/v1/project/1/workflow/", None),
        (GET, "/api/v1/project/1/workflow/1/", None),
        (GET, "/api/v1/project/1/workflow/1/export/", None),
        (GET, "/api/v1/workflow/", None),
        # WorkflowTask
        (GET, "/api/v1/project/1/workflow/1/wftask/1/", None),
        # Job
        (GET, "/api/v1/job/", None),
        (GET, "/api/v1/project/1/workflow/1/job/", None),
        (GET, "/api/v1/project/1/job/1/", None),
        (GET, "/api/v1/project/1/job/1/download/", None),
        (GET, "/api/v1/project/1/job/", None),
        (GET, "/api/v1/project/1/job/1/stop/", None),
    ]
    assert len(v1_endpoints) == 22

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True, "is_verified": True}
    ):
        for verb, route, payload in v1_endpoints:
            res = await verb(route)

            assert (
                res.status_code == 422
                and res.json()["detail"] == "Legacy API is in read-only mode."
            )
