import pytest
from devtools import debug

from fractal_server.app.runner import _backends
from fractal_server.app.runner._common import SHUTDOWN_FILENAME


backends_available = list(_backends.keys())


@pytest.mark.parametrize("backend", backends_available)
async def test_stop_job(
    backend,
    db,
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    workflow_factory,
    dataset_factory,
    tmp_path,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        wf = await workflow_factory(project_id=project.id)
        ds = await dataset_factory(project)
        wf_dump = {
            "name": "my workflow",
            "id": 1,
            "project_id": 1,
            "task_list": [],
        }
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=ds.id,
            output_dataset_id=ds.id,
            working_id=wf.id,
            workflow_dump=wf_dump,
        )

        debug(job)

        res = await client.get(
            f"api/v1/project/{project.id}/job/{job.id}/stop/"
        )
        if backend == "slurm":
            assert res.status_code == 200

            shutdown_file = tmp_path / SHUTDOWN_FILENAME
            debug(shutdown_file)
            assert shutdown_file.exists()
        else:
            assert res.status_code == 422
