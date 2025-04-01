# import time

import pytest
from devtools import debug

from ...aux_unit_runner import *  # noqa
from ...aux_unit_runner import ZARR_URLS
from fractal_server.app.runner.executors.slurm_ssh.runner import (
    RunnerSlurmSSH,
)
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.ssh
@pytest.mark.container
@pytest.mark.parametrize(
    "task_type",
    [
        "non_parallel",
        # "compound",
        # "converter_non_parallel",
        # "converter_compound",
    ],
)
async def test_submit_success(
    db,
    tmp777_path,
    fractal_ssh,
    history_mock_for_submit,
    override_settings_factory,
    task_type: str,
):
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON=None)

    def do_nothing(parameters: dict, **kwargs) -> int:
        return 42

    history_run_id, history_unit_id = history_mock_for_submit
    parameters = {"__FRACTAL_PARALLEL_COMPONENT__": "000000"}
    if not task_type.startswith("converter_"):
        parameters["zarr_urls"] = ZARR_URLS

    with RunnerSlurmSSH(
        fractal_ssh=fractal_ssh,
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(tmp777_path, component="0", is_slurm=True),
            slurm_config=get_default_slurm_config(),
            task_type=task_type,
        )
    debug(result, exception)
    assert result == 42
    assert exception is None


#
# @pytest.mark.container
# async def test_submit_fail(
#     db,
#     mock_history_item,
#     tmp777_path,
#     monkey_slurm,
# ):
#     ERROR_MSG = "very nice error"
#
#     def raise_ValueError(parameters: dict, **kwargs):
#         raise ValueError(ERROR_MSG)
#
#     with RunnerSlurmSudo(
#         slurm_user=SLURM_USER,
#         root_dir_local=tmp777_path / "server",
#         root_dir_remote=tmp777_path / "user",
#         slurm_poll_interval=0,
#     ) as runner:
#         result, exception = runner.submit(
#             raise_ValueError,
#             parameters={
#                 "zarr_urls": ZARR_URLS,
#                 "__FRACTAL_PARALLEL_COMPONENT__": "000000",
#             },
#             history_item_id=mock_history_item.id,
#             task_files=get_dummy_task_files(tmp777_path),
#             slurm_config=get_default_slurm_config(),
#         )
#
#     assert result is None
#     assert isinstance(exception, TaskExecutionError)
#     assert ERROR_MSG in str(exception)
#     db.expunge_all()
#
#     # Assertions on ImageStatus and HistoryItemV2 data
#     # wftask_id = mock_history_item.workflowtask_id
#     # dataset_id = mock_history_item.dataset_id
#     # for zarr_url in ZARR_URLS:
#     #     image_status = await db.get(
#     #         ImageStatus, (zarr_url, wftask_id, dataset_id)
#     #     )
#     #     assert image_status.status == ImageStatus.FAILED
#     # history_item = await db.get(HistoryItemV2, mock_history_item.id)
#     # assert history_item.images == {
#     #     zarr_url: ImageStatus.FAILED for zarr_url in ZARR_URLS
#     # }
#
#
# @pytest.mark.container
# async def test_multisubmit(
#     db,
#     mock_history_item,
#     tmp777_path,
#     monkey_slurm,
# ):
#     def fun(parameters: dict, **kwargs):
#         zarr_url = parameters["zarr_url"]
#         x = parameters["parameter"]
#         if x != 3:
#             print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
#             time.sleep(1)
#             return 2 * x
#         else:
#             print(f"Running with {zarr_url=} and {x=}, raising error.")
#             time.sleep(1)
#             raise ValueError("parameter=3 is very very bad")
#
#     with RunnerSlurmSudo(
#         slurm_user=SLURM_USER,
#         root_dir_local=tmp777_path / "server",
#         root_dir_remote=tmp777_path / "user",
#         slurm_poll_interval=0,
#     ) as runner:
#         results, exceptions = runner.multisubmit(
#             fun,
#             [
#                 {
#                     "zarr_url": "a",
#                     "parameter": 1,
#                     "__FRACTAL_PARALLEL_COMPONENT__": "000000",
#                 },
#                 {
#                     "zarr_url": "b",
#                     "parameter": 2,
#                     "__FRACTAL_PARALLEL_COMPONENT__": "000001",
#                 },
#                 {
#                     "zarr_url": "c",
#                     "parameter": 3,
#                     "__FRACTAL_PARALLEL_COMPONENT__": "000002",
#                 },
#                 {
#                     "zarr_url": "d",
#                     "parameter": 4,
#                     "__FRACTAL_PARALLEL_COMPONENT__": "000003",
#                 },
#             ],
#             history_item_id=mock_history_item.id,
#             task_files=get_dummy_task_files(tmp777_path),
#             slurm_config=get_default_slurm_config(),
#         )
#         debug(results)
#         debug(exceptions)
#     db.expunge_all()
#
#     # Assertions on ImageStatus and HistoryItemV2 data
#     # wftask_id = mock_history_item.workflowtask_id
#     # dataset_id = mock_history_item.dataset_id
#     # for zarr_url in ["a", "b", "d"]:
#     #     image_status = await db.get(
#     #         ImageStatus, (zarr_url, wftask_id, dataset_id)
#     #     )
#     #     assert image_status.status == ImageStatus.DONE
#     # for zarr_url in ["c"]:
#     #     image_status = await db.get(
#     #         ImageStatus, (zarr_url, wftask_id, dataset_id)
#     #     )
#     #     assert image_status.status == ImageStatus.FAILED
#     # history_item = await db.get(HistoryItemV2, mock_history_item.id)
#     # assert history_item.images == {
#     #     "a": "done",
#     #     "b": "done",
#     #     "c": "failed",
#     #     "d": "done",
#     # }
