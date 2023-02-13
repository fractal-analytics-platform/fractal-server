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

from devtools import debug

import fractal_server.tasks


PREFIX = "/api/v1"


async def test_full_workflow(
    db,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory,
    dataset_factory,
    override_settings_factory,
):
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="local",
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
    )

    async with MockCurrentUser(persist=True) as user:
        # add custom task
        task_path = f"{fractal_server.tasks.__path__[0]}/dummy.py"
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
        task_path = f"{fractal_server.tasks.__path__[0]}/dummy_parallel.py"
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
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # ADD TEST IMAGES AS RESOURCE TO INPUT DATASET

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{input_dataset_id}",
            json={
                "path": (testdata_path / "png").as_posix(),
                "glob_pattern": "*.png",
            },
        )
        debug(res.json())
        assert res.status_code == 201

        # CREATE OUTPUT DATASET AND RESOURCE

        res = await client.post(
            f"{PREFIX}/project/{project_id}/",
            json=dict(
                name="output dataset",
                type="json",
            ),
        )
        debug(res.json())
        assert res.status_code == 201
        output_dataset = res.json()
        output_dataset_id = output_dataset["id"]

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{output_dataset['id']}",
            json=dict(path=tmp777_path.as_posix(), glob_pattern="out.json"),
        )
        out_resource = res.json()
        debug(out_resource)
        assert res.status_code == 201

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project_id),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=task_id, args={"message": "my_message"}),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add a dummy_parallel task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(
                task_id=task_parallel_id, args={"message": "my_message"}
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
        res = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        res = await client.get(f"{PREFIX}/job/{job_data['id']}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"

        # Verify output
        res = await client.get(
            f"{PREFIX}/project/{project_id}/{output_dataset_id}"
        )
        data = res.json()
        debug(data)
        assert "dummy" in data["meta"]
        assert data["meta"]["history"][0] == TASK_NAME
        assert data["meta"]["history"][1].startswith(PARALLEL_TASK_NAME)
