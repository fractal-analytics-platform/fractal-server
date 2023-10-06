"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Yuri Chiucconi <yuri.chiucconi@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import sys

import pytest
from devtools import debug


PREFIX = "/api/v1"


@pytest.mark.parametrize(
    "override_settings",
    [{"FRACTAL_RUNNER_BACKEND": "local"}],
    indirect=True,
)
async def test_full_workflow(
    db,
    override_settings,
    override_settings_runtime,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory,
    dataset_factory,
):
    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=(tmp777_path / "artifacts")
    )
    async with MockCurrentUser(persist=True) as user:
        # add custom task
        task_path = str(testdata_path / "tasks_dummy/dummy.py")
        command = f"{sys.executable} {task_path}"
        TASK_NAME = "dummy_custom"
        payload = dict(
            name=TASK_NAME,
            command=command,
            source="my_source",
            input_type="Any",
            output_type="Any",
        )
        res = await client.post(f"{PREFIX}/task/", json=payload)
        debug(res.json())
        assert res.status_code == 201

        task_id = res.json()["id"]

        # add custom parallel task
        task_path = str(testdata_path / "tasks_dummy/dummy_parallel.py")
        command = f"{sys.executable} {task_path}"
        PARALLEL_TASK_NAME = "parallel_dummy_custom"
        payload = dict(
            name=PARALLEL_TASK_NAME,
            command=command,
            source="my_other_source",
            input_type="Any",
            output_type="Any",
            meta={"parallelization_level": "index"},
        )
        res = await client.post(f"{PREFIX}/task/", json=payload)
        debug(res.json())
        assert res.status_code == 201

        task_parallel_id = res.json()["id"]

        project = await project_factory(user)
        debug(project)
        project_id = project.id
        input_dataset = await dataset_factory(
            project_id=project.id, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # Add a resource to input_dataset_id
        res = await client.post(
            (
                f"{PREFIX}/project/{project_id}"
                f"/dataset/{input_dataset_id}/resource/"
            ),
            json={
                "path": (testdata_path / "png").as_posix(),
            },
        )
        debug(res.json())
        assert res.status_code == 201

        # Create output dataset
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/",
            json=dict(
                name="output dataset",
                type="json",
            ),
        )
        output_dataset = res.json()
        debug(output_dataset)
        assert res.status_code == 201
        output_dataset_id = output_dataset["id"]

        # Add a resource
        res = await client.post(
            (
                f"{PREFIX}/project/{project_id}/"
                f"dataset/{output_dataset_id}/resource/"
            ),
            json=dict(path=tmp777_path.as_posix()),
        )
        out_resource = res.json()
        debug(out_resource)
        assert res.status_code == 201

        # Create workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/",
            json=dict(name="test workflow"),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            (
                f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/"
                f"wftask/?task_id={task_id}"
            ),
            json=dict(args={"message": "my_message"}),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add a dummy_parallel task
        res = await client.post(
            (
                f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/"
                f"wftask/?task_id={task_parallel_id}"
            ),
            json=dict(args={"message": "my_message"}),
        )
        debug(res.json())
        assert res.status_code == 201

        # Execute workflow
        url = (
            f"{PREFIX}/project/{project_id}/"
            f"workflow/{workflow_id}/apply/"
            f"?input_dataset_id={input_dataset_id}"
            f"&output_dataset_id={output_dataset_id}"
        )
        debug(url)
        res = await client.post(url, json={})
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"

        # Verify output
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{output_dataset_id}"
        )
        data = res.json()
        debug(data)
        assert "dummy" in data["meta"]
        history = data["meta"]["history"]
        assert history[0]["workflowtask"]["task"]["name"] == TASK_NAME
        assert (
            history[1]["workflowtask"]["task"]["name"] == PARALLEL_TASK_NAME
        )  # noqa
