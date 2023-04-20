from pathlib import Path
from zipfile import ZipFile

from devtools import debug

PREFIX = "/api/v1/job"


async def test_job_download_logs(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        # Create all needed objects in the database
        input_dataset = await dataset_factory(prj, name="input")
        output_dataset = await dataset_factory(prj, name="output")
        workflow = await workflow_factory(project_id=prj.id)
        working_dir = (tmp_path / "workflow_dir_for_zipping").as_posix()
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )
        debug(job)

        # Write a log file in working_dir
        LOG_CONTENT = "This is a log\n"
        LOG_FILE = "log.txt"
        Path(working_dir).mkdir()
        with (Path(working_dir) / LOG_FILE).open("w") as f:
            f.write(LOG_CONTENT)

        # Test that the endpoint returns a list with the new job
        res = await client.get(f"/api/v1/job/download/{job.id}")
        assert res.status_code == 200
        assert (
            res.headers.get("content-type") == "application/x-zip-compressed"
        )

        # Write response into a zipped file
        zipped_archive_path = tmp_path / "logs.zip"
        debug(zipped_archive_path)
        with zipped_archive_path.open("wb") as f:
            f.write(res.content)

        # Unzip the log archive
        unzipped_archived_path = tmp_path / "unzipped_logs"
        debug(unzipped_archived_path)
        with ZipFile(zipped_archive_path, mode="r") as zipfile:
            zipfile.extractall(path=unzipped_archived_path)

        # Verify content of the unzipped log archive
        with (unzipped_archived_path / LOG_FILE).open("r") as f:
            actual_logs = f.read()
        assert LOG_CONTENT in actual_logs
