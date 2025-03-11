# import os
# import shlex
# import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from devtools import debug

from ...aux_unit_runner import ZARR_URLS
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.models.v2.history import HistoryItemV2
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    RunnerSlurmSudo,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.task_files import TaskFiles
from tests.fixtures_slurm import run_squeue
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config


SCANCEL_CMD = (
    f"sudo --non-interactive -u {SLURM_USER} scancel -u {SLURM_USER} -v"
)


def get_dummy_task_files(root_path: Path) -> TaskFiles:
    return TaskFiles(
        root_dir_local=root_path / "server",
        root_dir_remote=root_path / "user",
        task_name="name",
        task_order=0,
    )


@pytest.fixture
async def mock_history_item(  # FIXME de-duplicate
    db,
    project_factory_v2,
    dataset_factory_v2,
    MockCurrentUser,
):
    # Create test data
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user=user)
        dataset = await dataset_factory_v2(project_id=project.id)
    item = HistoryItemV2(
        dataset_id=dataset.id,
        workflowtask_id=None,
        workflowtask_dump={},
        task_group_dump={},
        parameters_hash="xxx",
        num_current_images=4,
        num_available_images=4,
        images={
            zarr_url: HistoryItemImageStatus.SUBMITTED
            for zarr_url in ZARR_URLS
        },
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


def _write_shutdown_file(
    shutdown_file: Path,
    grace_time: float,
):
    squeue_output = run_squeue(squeue_format="%i %T")
    while "RUNNING" not in squeue_output:
        time.sleep(0.2)
        squeue_output = run_squeue(squeue_format="%i %T")
    debug(squeue_output)
    time.sleep(grace_time)
    debug(f"Now create {shutdown_file}")
    shutdown_file.touch()


# @pytest.mark.xfail(reason="Not ready - FIXME")
@pytest.mark.container
async def test_shutdown_during_submit(
    db,
    mock_history_item,
    tmp777_path,
    monkey_slurm,
):
    def sleep_long(**parameters: dict):
        time.sleep(parameters["sleep_time"])

    (tmp777_path / "server").mkdir()

    def main_thread():
        with RunnerSlurmSudo(
            slurm_user=SLURM_USER,
            root_dir_local=tmp777_path / "server",
            root_dir_remote=tmp777_path / "user",
            slurm_poll_interval=0,
        ) as runner:
            result, exception = runner.submit(
                sleep_long,
                parameters={
                    "zarr_urls": ZARR_URLS,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000000",
                    "sleep_time": 5000,
                },
                history_item_id=mock_history_item.id,
                task_files=get_dummy_task_files(tmp777_path),
                slurm_config=get_default_slurm_config(),
            )
            return result, exception

    shutdown_file = tmp777_path / "server" / SHUTDOWN_FILENAME

    with ThreadPoolExecutor(max_workers=2) as executor:
        fut1 = executor.submit(main_thread)
        fut2 = executor.submit(_write_shutdown_file, shutdown_file, 0.5)
        fut2.result()
        result, exception = fut1.result()
        debug(result, exception)
        # assert "Fractal job was shut down" in str(exception)

    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.FAILED for zarr_url in ZARR_URLS
    }


@pytest.mark.xfail(reason="Not ready - FIXME")
@pytest.mark.container
async def test_shutdown_during_multisubmit(
    db,
    mock_history_item,
    tmp777_path,
    monkey_slurm,
):
    def sleep_long(parameters: dict):
        time.sleep(parameters["sleep_time"])

    def main_thread():
        with RunnerSlurmSudo(
            slurm_user=SLURM_USER,
            root_dir_local=tmp777_path / "server",
            root_dir_remote=tmp777_path / "user",
            slurm_poll_interval=0,
        ) as runner:
            results, exceptions = runner.multisubmit(
                sleep_long,
                list_parameters=[
                    dict(
                        zarr_url=zarr_url,
                        sleep_time=100,
                    )
                    for zarr_url in ZARR_URLS
                ],
                history_item_id=mock_history_item.id,
                workdir_local=tmp777_path / "server/task",
                workdir_remote=tmp777_path / "user/task",
            )
            return results, exceptions

    shutdown_file = tmp777_path / "server" / SHUTDOWN_FILENAME

    with ThreadPoolExecutor(max_workers=2) as executor:
        fut1 = executor.submit(main_thread)
        fut2 = executor.submit(_write_shutdown_file, shutdown_file, 2)
        fut2.result()
        results, exceptions = fut1.result()
        debug(results, exceptions)
        # assert "Fractal job was shut down" in str(exceptions)

    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.FAILED for zarr_url in ZARR_URLS
    }


# @pytest.mark.container
# async def test_scancel_during_execution(
#     tmp777_path: Path, monkey_slurm, slurm_working_folders
# ):
#     """
#     Test the scenario where `scancel` is called during a
#     `FractalSlurmExecutor` execution of `.submit` or `.map`.
#     """
#     # Define and create folders and subfolders for both cases
#     base_dir_local, base_dir_remote = slurm_working_folders
#     job_folders = {}
#     for job_name in ["job1", "job2"]:
#         job_dir_local = base_dir_local / job_name
#         job_dir_remote = base_dir_remote / job_name
#         job_folders[job_name] = dict(
#             local=job_dir_local,
#             remote=job_dir_remote,
#         )
#         task_files = get_default_task_files(
#             workflow_dir_local=job_dir_local,
#             workflow_dir_remote=job_dir_remote,
#         )
#         umask = os.umask(0)
#         (job_dir_local / task_files.subfolder_name).mkdir(parents=True)
#         os.umask(umask)
#         _mkdir_as_user(
#             folder=str(job_dir_remote / task_files.subfolder_name),
#             user=SLURM_USER,
#         )

#     scancel_cmd = f"sudo --non-interactive -u {SLURM_USER} scancel -u {SLURM_USER} -v"  # noqa

#     # JOB 1: fail during `submit`
#     with pytest.raises(JobExecutionError) as exc_info:
#         with RunnerSlurmSudo(
#             root_dir_local=job_folders["job1"]["local"],
#             root_dir_remote=job_folders["job1"]["remote"],
#             slurm_user=SLURM_USER,
#             slurm_poll_interval=1,
#         ) as executor:
#             # Submit task
#             fut = executor.submit(
#                 time.sleep,
#                 100,
#                 slurm_config=get_default_slurm_config(),
#                 task_files=task_files,
#             )
#             # Wait and then scancel
#             while "RUNNING" not in run_squeue(squeue_format="%i %T"):
#                 time.sleep(0.1)
#             subprocess.run(
#                 shlex.split(scancel_cmd), capture_output=True, encoding="utf-8"  # noqa
#             )
#             # Trigger exception
#             fut.result()
#     job_execution_error = exc_info.value
#     assert "CANCELLED" in job_execution_error.assemble_error()

#     # JOB 2: fail during `map`
#     with pytest.raises(JobExecutionError) as exc_info:
#         with RunnerSlurmSudo(
#             root_dir_local=job_folders["job2"]["local"],
#             root_dir_remote=job_folders["job2"]["remote"],
#             slurm_user=SLURM_USER,
#             slurm_poll_interval=1,
#         ) as executor:
#             # Submit task
#             res = executor.map(
#                 time.sleep,
#                 [100, 101],
#                 slurm_config=get_default_slurm_config(),
#                 task_files=task_files,
#             )
#             # Wait and then scancel
#             while "RUNNING" not in run_squeue(squeue_format="%i %T"):
#                 time.sleep(0.1)
#             subprocess.run(
#                 shlex.split(scancel_cmd), capture_output=True, encoding="utf-8"  # noqa
#             )
#             # Trigger exception
#             list(res)
#     job_execution_error = exc_info.value
#     assert "CANCELLED" in job_execution_error.assemble_error()
