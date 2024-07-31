async def test_read_only_enpoints(
    client, override_settings_factory, MockCurrentUser
):

    override_settings_factory(FRACTAL_API_V1_MODE="include_read_only")

    GET = client.get
    POST = client.post
    PATCH = client.patch
    DELETE = client.delete

    v1_endpoints = [
        # Project
        (GET, "/api/v1/project/", None),
        (POST, "/api/v1/project/", dict(name="foo")),
        (GET, "/api/v1/project/1/", None),
        (DELETE, "/api/v1/project/1/", None),
        (PATCH, "/api/v1/project/1/", dict()),
        (
            POST,
            "/api/v1/project/1/workflow/1/apply/"
            "?input_dataset_id=1&output_dataset_id=1",
            dict(),
        ),
        # Task
        (GET, "/api/v1/task/", None),
        (
            POST,
            "/api/v1/task/",
            dict(
                name="foo",
                source="foo",
                command="foo",
                input_type="foo",
                output_type="foo",
            ),
        ),
        (GET, "/api/v1/task/1/", None),
        (DELETE, "/api/v1/task/1/", None),
        (PATCH, "/api/v1/task/1/", dict()),
        # Task Collection
        (POST, "/api/v1/task/collect/pip/", dict(package="foo")),
        (GET, "/api/v1/task/collect/1/", None),
        # Dataset
        (GET, "/api/v1/project/1/dataset/", None),
        (POST, "/api/v1/project/1/dataset/", dict(name="foo")),
        (GET, "/api/v1/project/1/dataset/1/", None),
        (DELETE, "/api/v1/project/1/dataset/1/", None),
        (PATCH, "/api/v1/project/1/dataset/1/", dict()),
        (GET, "/api/v1/project/1/dataset/1/resource/", None),
        (POST, "/api/v1/project/1/dataset/1/resource/", dict(path="/foo")),
        (DELETE, "/api/v1/project/1/dataset/1/resource/1/", None),
        (PATCH, "/api/v1/project/1/dataset/1/resource/1/", dict(path="/foo")),
        (GET, "/api/v1/project/1/dataset/1/export_history/", None),
        (GET, "/api/v1/project/1/dataset/1/status/", None),
        (GET, "/api/v1/dataset/", None),
        # Workflow
        (GET, "/api/v1/project/1/workflow/", None),
        (POST, "/api/v1/project/1/workflow/", dict(name="foo")),
        (GET, "/api/v1/project/1/workflow/1/", None),
        (DELETE, "/api/v1/project/1/workflow/1/", None),
        (PATCH, "/api/v1/project/1/workflow/1/", dict()),
        (GET, "/api/v1/project/1/workflow/1/export/", None),
        (
            POST,
            "/api/v1/project/1/workflow/import/",
            dict(name="foo", task_list=[]),
        ),
        (GET, "/api/v1/workflow/", None),
        # WorkflowTask
        (POST, "/api/v1/project/1/workflow/1/wftask/?task_id=1", dict()),
        (GET, "/api/v1/project/1/workflow/1/wftask/1/", None),
        (DELETE, "/api/v1/project/1/workflow/1/wftask/1/", None),
        (PATCH, "/api/v1/project/1/workflow/1/wftask/1/", dict()),
        # Job
        (GET, "/api/v1/job/", None),
        (GET, "/api/v1/project/1/workflow/1/job/", None),
        (GET, "/api/v1/project/1/job/1/", None),
        (GET, "/api/v1/project/1/job/1/download/", None),
        (GET, "/api/v1/project/1/job/", None),
        (GET, "/api/v1/project/1/job/1/stop/", None),
    ]
    assert len(v1_endpoints) == 43

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True, "is_verified": True}
    ):
        for verb, route, payload in v1_endpoints:

            if payload is not None:
                res = await verb(route, json=payload)
            else:
                res = await verb(route)

            if verb is not GET:
                assert (
                    res.status_code == 422
                    and res.json()["detail"]
                    == "Legacy API is in read-only mode."
                )
            else:
                assert (
                    res.status_code != 422
                    or res.json()["detail"]
                    != "Legacy API is in read-only mode."
                )
