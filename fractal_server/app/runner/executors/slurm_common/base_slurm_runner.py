import math
import time
from pathlib import Path
from typing import Any
from typing import Literal

import cloudpickle

from ..slurm_common._slurm_config import SlurmConfig
from ..slurm_common.slurm_job_task_models import SlurmJob
from ..slurm_common.slurm_job_task_models import SlurmTask
from ._batching import heuristics
from ._handle_exception_proxy import _handle_exception_proxy
from fractal_server.app.db import get_sync_db
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.logger import set_logger

logger = set_logger(__name__)


class BaseSlurmRunner(BaseRunner):
    shutdown_file: Path
    common_script_lines: list[str]
    user_cache_dir: str
    root_dir_local: Path
    root_dir_remote: Path
    poll_interval: int
    python_worker_interpreter: str
    jobs: dict[str, SlurmJob]
    slurm_poll_interval: int

    def _get_finished_jobs(self, job_ids: list[str]) -> set[str]:
        raise NotImplementedError("Implement in child class.")

    def _mkdir_local_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _mkdir_remote_folder(self, folder: str) -> None:
        raise NotImplementedError("Implement in child class.")

    def _submit_single_sbatch(
        self,
        func,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
        raise NotImplementedError("Implement in child class.")

    def _copy_files_from_remote_to_local(
        self,
        slurm_job: SlurmJob,
    ) -> None:
        raise NotImplementedError("Implement in child class.")

    def _postprocess_single_task(
        self, *, task: SlurmTask
    ) -> tuple[Any, Exception]:
        try:
            with open(task.output_pickle_file_local, "rb") as f:
                outdata = f.read()
            success, output = cloudpickle.loads(outdata)
            if success:
                result = output
                return result, None
            else:
                exception = _handle_exception_proxy(output)
                return None, exception
        except Exception as e:
            exception = JobExecutionError(f"ERROR, {str(e)}")
            return None, exception
        finally:
            Path(task.input_pickle_file_local).unlink(missing_ok=True)
            Path(task.output_pickle_file_local).unlink(missing_ok=True)

    def scancel_jobs(self) -> None:
        raise NotImplementedError("Implement in child class.")

    def is_shutdown(self) -> bool:
        # FIXME: shutdown is not implemented
        return self.shutdown_file.exists()

    @property
    def job_ids(self) -> list[str]:
        return list(self.jobs.keys())

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_files: TaskFiles,
        config: SlurmConfig,
        task_type: Literal[
            "non_parallel",
            "converter_non_parallel",
            "compound",
            "converter_compound",
        ],
    ) -> tuple[Any, Exception]:

        workdir_local = task_files.wftask_subfolder_local
        workdir_remote = task_files.wftask_subfolder_remote

        if self.jobs != {}:
            raise JobExecutionError("Unexpected branch: jobs should be empty.")

        if self.is_shutdown():
            raise JobExecutionError("Cannot continue after shutdown.")

        # Validation phase
        self.validate_submit_parameters(
            parameters=parameters,
            task_type=task_type,
        )

        # Create task subfolder
        self._mkdir_local_folder(folder=workdir_local.as_posix())
        self._mkdir_remote_folder(folder=workdir_remote.as_posix())

        # Submission phase
        slurm_job = SlurmJob(
            label="0",
            workdir_local=workdir_local,
            workdir_remote=workdir_remote,
            tasks=[
                SlurmTask(
                    index=0,
                    component=task_files.component,
                    parameters=parameters,
                    workdir_remote=workdir_remote,
                    workdir_local=workdir_local,
                    task_files=task_files,
                )
            ],
        )

        config.parallel_tasks_per_job = 1
        self._submit_single_sbatch(
            func,
            slurm_job=slurm_job,
            slurm_config=config,
        )
        logger.info(f"END submission phase, {self.job_ids=}")

        # TODO: check if this sleep is necessary
        logger.warning("Now sleep 4 (FIXME)")
        time.sleep(4)

        # Retrieval phase
        logger.info("START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = self._get_finished_jobs(job_ids=self.job_ids)
            logger.debug(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.debug(f"Now process {slurm_job_id=}")
                    slurm_job = self.jobs.pop(slurm_job_id)

                    self._copy_files_from_remote_to_local(slurm_job)
                    result, exception = self._postprocess_single_task(
                        task=slurm_job.tasks[0]
                    )
                    if exception is not None:
                        update_status_of_history_unit(
                            history_unit_id=history_unit_id,
                            status=HistoryUnitStatus.FAILED,
                            db_sync=db,
                        )
                    else:
                        if task_type not in ["compound", "converter_compound"]:
                            update_status_of_history_unit(
                                history_unit_id=history_unit_id,
                                status=HistoryUnitStatus.DONE,
                                db_sync=db,
                            )

            time.sleep(self.slurm_poll_interval)

        return result, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: Literal["parallel", "compound", "converter_compound"],
        config: SlurmConfig,
    ):

        if len(self.jobs) > 0:
            raise RuntimeError(
                f"Cannot run .multisubmit when {len(self.jobs)=}"
            )

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            task_type=task_type,
            list_task_files=list_task_files,
        )
        self.validate_multisubmit_history_unit_ids(
            history_unit_ids=history_unit_ids,
            task_type=task_type,
            list_parameters=list_parameters,
        )

        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")

        workdir_local = list_task_files[0].wftask_subfolder_local
        workdir_remote = list_task_files[0].wftask_subfolder_remote

        # Create local&remote task subfolders
        if task_type == "parallel":
            self._mkdir_local_folder(workdir_local.as_posix())
            self._mkdir_remote_folder(folder=workdir_remote.as_posix())

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        # TODO Pick a data structure for results and exceptions, or review the
        # interface
        results: dict[int, Any] = {}
        exceptions: dict[int, BaseException] = {}

        original_task_files = list_task_files
        tot_tasks = len(list_parameters)

        # Set/validate parameters for task batching
        tasks_per_job, parallel_tasks_per_job = heuristics(
            # Number of parallel components (always known)
            tot_tasks=tot_tasks,
            # Optional WorkflowTask attributes:
            tasks_per_job=config.tasks_per_job,
            parallel_tasks_per_job=config.parallel_tasks_per_job,  # noqa
            # Task requirements (multiple possible sources):
            cpus_per_task=config.cpus_per_task,
            mem_per_task=config.mem_per_task_MB,
            # Fractal configuration variables (soft/hard limits):
            target_cpus_per_job=config.target_cpus_per_job,
            target_mem_per_job=config.target_mem_per_job,
            target_num_jobs=config.target_num_jobs,
            max_cpus_per_job=config.max_cpus_per_job,
            max_mem_per_job=config.max_mem_per_job,
            max_num_jobs=config.max_num_jobs,
        )
        config.parallel_tasks_per_job = parallel_tasks_per_job
        config.tasks_per_job = tasks_per_job

        # Divide arguments in batches of `tasks_per_job` tasks each
        args_batches = []
        batch_size = tasks_per_job
        for ind_chunk in range(0, tot_tasks, batch_size):
            args_batches.append(
                list_parameters[ind_chunk : ind_chunk + batch_size]  # noqa
            )
        if len(args_batches) != math.ceil(tot_tasks / tasks_per_job):
            raise RuntimeError("Something wrong here while batching tasks")

        logger.info(f"START submission phase, {list(self.jobs.keys())=}")
        for ind_batch, chunk in enumerate(args_batches):
            tasks = []
            for ind_chunk, parameters in enumerate(chunk):
                index = (ind_batch * batch_size) + ind_chunk
                tasks.append(
                    SlurmTask(
                        index=index,
                        component=original_task_files[index].component,
                        workdir_local=workdir_local,
                        workdir_remote=workdir_remote,
                        parameters=parameters,
                        zarr_url=parameters["zarr_url"],
                        task_files=original_task_files[index],
                    ),
                )

            slurm_job = SlurmJob(
                label=f"{ind_batch:06d}",
                workdir_local=workdir_local,
                workdir_remote=workdir_remote,
                tasks=tasks,
            )
            self._submit_single_sbatch(
                func,
                slurm_job=slurm_job,
                slurm_config=config,
            )
        logger.info(f"END submission phase, {self.job_ids=}")

        # FIXME: Replace with more robust/efficient logic
        logger.warning("Now sleep 4 (FIXME)")
        time.sleep(4)

        # Retrieval phase
        logger.info("START retrieval phase")
        while len(self.jobs) > 0:
            if self.is_shutdown():
                self.scancel_jobs()
            finished_job_ids = self._get_finished_jobs(job_ids=self.job_ids)
            logger.debug(f"{finished_job_ids=}")
            with next(get_sync_db()) as db:
                for slurm_job_id in finished_job_ids:
                    logger.debug(f"Now processing {slurm_job_id=}")
                    slurm_job = self.jobs.pop(slurm_job_id)
                    self._copy_files_from_remote_to_local(slurm_job)
                    for task in slurm_job.tasks:
                        logger.debug(f"Now processing {task.index=}")
                        result, exception = self._postprocess_single_task(
                            task=task
                        )

                        # Note: the relevant done/failed check is based on
                        # whether `exception is None`. The fact that
                        # `result is None` is not relevant for this purpose.
                        if exception is not None:
                            logger.debug(
                                f"Task {task.index} has an exception."
                            )  # FIXME  # noqa
                            exceptions[task.index] = exception
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.FAILED,
                                    db_sync=db,
                                )
                        else:
                            logger.debug(
                                f"Task {task.index} has no exception."
                            )  # FIXME  # noqa
                            results[task.index] = result
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=history_unit_ids[
                                        task.index
                                    ],
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

            time.sleep(self.slurm_poll_interval)
        return results, exceptions
