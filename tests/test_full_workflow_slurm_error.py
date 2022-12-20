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
import subprocess
import time

import pytest
from devtools import debug

try:
    import cfut

    HAS_SLURM = True
    reason = "All good"
except ModuleNotFoundError as e:
    reason = str(e)
    HAS_SLURM = False


PREFIX = "/api/v1"


@pytest.mark.slow
@pytest.mark.skipif(not HAS_SLURM, reason=reason)
async def test_failing_workflow_slurm_error(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    request,
    override_settings_factory,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    request.getfixturevalue("monkey_slurm")
    request.getfixturevalue("relink_python_interpreter")

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # CREATE OUTPUT DATASET AND RESOURCE

        res = await client.post(
            f"{PREFIX}/project/{project_id}/",
            json=dict(
                name="output dataset",
                type="json",
            ),
        )
        assert res.status_code == 201
        output_dataset = res.json()
        output_dataset_id = output_dataset["id"]

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{output_dataset['id']}",
            json=dict(path=tmp777_path.as_posix(), glob_pattern="out.json"),
        )
        assert res.status_code == 201

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project.id),
        )
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(
                task_id=collect_packages[0].id,
                args={"raise_error": False, "sleep_time": 100},
                meta={"executor": "cpu-low-1-sec"},
            ),
        )
        assert res.status_code == 201

        # EXECUTE WORKFLOW

        payload = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            overwrite_input=False,
        )
        res = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
        assert res.status_code == 202
        job_data = res.json()
        debug(job_data)
        job_id = job_data["id"]

        # FIXME: how do we know that job ID is 2?
        time.sleep(1)
        debug("NOW SCANCEL")
        subprocess.run(
            ["sudo", "--non-interactive", "-u test01", "scancel", "2"]
        )

        res = await client.get(f"{PREFIX}/job/{job_id}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "failed"
        assert "id: None" not in job_status_data["log"]
        # TODO add check on log content.
