import pytest
from devtools import debug

from fractal_server.app.runner import _backends
from fractal_server.app.runner._common import SHUTDOWN_FILENAME


backends_available = list(_backends.keys())


@pytest.mark.parametrize(
    "override_settings, backend",
    [
        ({"FRACTAL_RUNNER_BACKEND": backend}, backend)
        for backend in backends_available
    ],
    indirect=["override_settings"],
)
async def test_stop_job(
    db,
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    workflow_factory,
    dataset_factory,
    tmp_path,
    task_factory,
    override_settings,
    backend,
):

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        wf = await workflow_factory(project_id=project.id)
        t = await task_factory(name="task", source="source")
        ds = await dataset_factory(project_id=project.id)
        await wf.insert_task(task_id=t.id, db=db)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=ds.id,
            output_dataset_id=ds.id,
            working_id=wf.id,
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
