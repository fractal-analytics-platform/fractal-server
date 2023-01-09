import time

from devtools import debug

PREFIX = "/api/v1"


async def test_async_apply_endpoint(
    db,
    client,
    MockCurrentUser,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
):

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        debug(project)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id
        output_dataset = await dataset_factory(
            project, name="output", type="image", read_only=True
        )
        output_dataset_id = output_dataset.id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/{output_dataset_id}",
            json=dict(path=tmp777_path.as_posix(), glob_pattern="*.json"),
        )
        assert res.status_code == 201

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project_id),
        )
        assert res.status_code == 201
        workflow_id = res.json()["id"]

        # Add a dummy task
        SLEEP_TIME = 5
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(
                task_id=collect_packages[0].id, args={"sleep_time": SLEEP_TIME}
            ),
        )
        debug(res.json())
        assert res.status_code == 201

        # EXECUTE WORKFLOW
        payload = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            overwrite_input=False,
        )
        debug(payload)

        t0 = time.perf_counter()
        res = await client.post(f"{PREFIX}/project/apply/", json=payload)
        elapsed = time.perf_counter() - t0
        debug(f"Elapsed request time: {elapsed} s")
        assert elapsed < SLEEP_TIME
