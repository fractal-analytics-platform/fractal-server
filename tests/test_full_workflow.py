"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
from os import environ

import pytest
from devtools import debug

from .fixtures_server import get_patched_settings
from fractal_server.app.runner import _backends
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


PREFIX = "/api/v1"

environ["RUNNER_MONITORING"] = "0"

backends_available = list(_backends.keys())


@pytest.mark.slow
@pytest.mark.parametrize("backend", backends_available)
async def test_full_workflow(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    backend,
    request,
):

    # Override RUNNER_BACKEND variable
    settings = get_patched_settings(tmp777_path)
    settings.RUNNER_BACKEND = backend
    if backend == "slurm":
        settings.FRACTAL_SLURM_CONFIG_FILE = (
            testdata_path / "slurm_config.json"
        )

    def _get_settings():
        return settings

    Inject.override(get_settings, _get_settings)

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("slurm_config")

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        debug(project)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # EDIT DEFAULT DATASET TO SET TYPE IMAGE

        res = await client.patch(
            f"{PREFIX}/project/{project_id}/{input_dataset_id}",
            json={"type": "image", "read_only": True},
        )
        debug(res.json())
        assert res.status_code == 200

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

        # CHECK WHERE WE ARE AT
        res = await client.get(f"{PREFIX}/project/{project_id}")
        debug(res.json())
        project_dict = res.json()

        # Workaround: for one backend, create a dummy workflow, so that the
        # actual one will have ID=2 (rather than 1). In this way, the workflow
        # folders for the two test runs (with the two different backends) won't
        # have the same name
        num_empty_workflows = backends_available.index(backend)
        for ind in range(num_empty_workflows):
            _ = await client.post(
                f"{PREFIX}/workflow/",
                json=dict(
                    name=f"workaround - {ind}",
                    project_id=project_dict["id"],
                ),
            )

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project_dict["id"]),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[0].id),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add another (identical) dummy task, to make sure that this is allowed
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[0].id),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add a dummy_parallel task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[1].id),
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
        debug(res.json())
        assert res.status_code == 202

        # Verify output
        res = await client.get(
            f"{PREFIX}/dataset/{project_id}/{output_dataset_id}"
        )
        data = res.json()
        debug(data)
        assert "dummy" in data["meta"]
