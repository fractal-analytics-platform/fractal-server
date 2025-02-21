import logging
import shlex
import subprocess
import sys
import time
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from typing import Optional

import cloudpickle
from pydantic import BaseModel

from ._check_jobs_status import _jobs_finished
from fractal_server import __VERSION__
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

TASK_LOG_FILE = "/tmp/slurm/task.log"


def _handle_exception_proxy(proxy):
    if proxy.exc_type_name == "JobExecutionError":
        return JobExecutionError(str(proxy))
    else:
        kwargs = {}
        for key in [
            "workflow_task_id",
            "workflow_task_order",
            "task_name",
        ]:
            if key in proxy.kwargs.keys():
                kwargs[key] = proxy.kwargs[key]
        return TaskExecutionError(proxy.tb, **kwargs)


class SlurmTask(BaseModel):
    input_pickle_file_local: str
    input_pickle_file_remote: str
    output_pickle_file_local: str
    output_pickle_file_remote: str
    task_log_file_local: str = TASK_LOG_FILE
    task_log_file_remote: str = TASK_LOG_FILE
    zarr_url: Optional[str] = None


class SlurmJob(BaseModel):
    slurm_job_id: Optional[str] = None
    slurm_log_file_local: str
    slurm_log_file_remote: str
    slurm_submission_script_local: str
    slurm_submission_script_remote: str
    tasks: tuple[SlurmTask]


def _subprocess_run_or_raise(full_command: str) -> Optional[CompletedProcess]:
    try:
        output = subprocess.run(  # nosec
            shlex.split(full_command),
            capture_output=True,
            check=True,
            encoding="utf-8",
        )
        return output
    except subprocess.CalledProcessError as e:
        error_msg = (
            f"Submit command `{full_command}` failed. "
            f"Original error:\n{str(e)}\n"
            f"Original stdout:\n{e.stdout}\n"
            f"Original stderr:\n{e.stderr}\n"
        )
        logging.error(error_msg)
        raise JobExecutionError(info=error_msg)


class RunnerSlurmSudo(BaseRunner):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __init__(self):
        self.slurm_user = "test01"

    def submit_single_sbatch(
        self,
        func,
        parameters,
        slurm_job: SlurmJob,
    ) -> str:
        if len(slurm_job.tasks) > 1:
            raise NotImplementedError()

        # Prepare input pickle(s)
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        for task in slurm_job.tasks:
            _args = []
            # TODO: make parameters task-dependent
            _kwargs = dict(parameters=parameters)
            funcser = cloudpickle.dumps((versions, func, _args, _kwargs))
            with open(task.input_pickle_file_local, "wb") as f:
                f.write(funcser)

        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        preamble_lines = [
            "#!/bin/bash",
            "#SBATCH --partition=main",
            "#SBATCH --ntasks=1",
            "#SBATCH --cpus-per-task=1",
            "#SBATCH --mem=10M",
            f"#SBATCH --err={slurm_job.slurm_log_file_remote}",
            f"#SBATCH --out={slurm_job.slurm_log_file_remote}",
            "#SBATCH -D /tmp/slurm/",
            "#SBATCH --job-name=test",
            "\n",
        ]

        cmdlines = []
        for task in slurm_job.tasks:
            cmd = (
                f"{python_worker_interpreter}"
                " -m fractal_server.app.runner.executors.slurm_common.remote "
                f"--input-file {task.input_pickle_file_remote} "
                f"--output-file {task.output_pickle_file_remote}"
            )
            cmdlines.append(
                f"srun --ntasks=1 --cpus-per-task=1 --mem=10MB {cmd} &"
            )
        cmdlines.append("wait\n")

        # Write submission script
        submission_script_contents = "\n".join(preamble_lines + cmdlines)
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(submission_script_contents)

        # Run sbatch
        pre_command = f"sudo --set-home --non-interactive -u {self.slurm_user}"
        submit_command = (
            f"sbatch --parsable {slurm_job.slurm_submission_script_remote}"
        )
        full_command = f"{pre_command} {submit_command}"

        # Submit SLURM job and retrieve job ID
        res = _subprocess_run_or_raise(full_command)
        job_id = int(res.stdout)
        return str(job_id)

    def process_single_task(
        self,
        *,
        task: SlurmTask,
    ) -> tuple[Any, Exception]:
        try:
            # TODO: Copy files from remote to local folder
            Path(task.input_pickle_file_local).unlink()
            if not Path(task.output_pickle_file_local).exists():
                # TODO: Prepare appropriate exception
                exception = JobExecutionError(
                    f"Missing {task.output_pickle_file_local}"
                )
                return None, exception
            else:
                with open(task.output_pickle_file_local, "rb") as f:
                    outdata = f.read()
                success, output = cloudpickle.loads(outdata)
                Path(task.output_pickle_file_local).unlink()
                if success:
                    result = output
                    return result, None
                else:
                    exception = _handle_exception_proxy(output)
                    return None, exception
        except Exception:
            exception = JobExecutionError("unclear")
            return None, exception

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_item_id: int,
        in_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, Exception]:
        # Validation phase
        self.validate_submit_parameters(parameters)

        # Submission phase
        slurm_job = SlurmJob(
            slurm_log_file_local="/tmp/slurm/slurm.log",
            slurm_log_file_remote="/tmp/slurm/slurm.log",
            slurm_submission_script_local="/tmp/slurm/submit.sh",
            slurm_submission_script_remote="/tmp/slurm/submit.sh",
            tasks=[
                SlurmTask(
                    input_pickle_file_local="/tmp/slurm/input.pickle",
                    input_pickle_file_remote="/tmp/slurm/input.pickle",
                    output_pickle_file_local="/tmp/slurm/output.pickle",
                    output_pickle_file_remote="/tmp/slurm/output.pickle",
                )
            ],
        )  # TODO: replace with actual values
        slurm_job_id = self.submit_single_sbatch(
            func,
            parameters=parameters,
            slurm_job=slurm_job,
        )
        slurm_job.slurm_job_id = slurm_job_id
        jobs = {slurm_job_id: slurm_job}

        # Retrieval phase
        while len(jobs) > 0:
            # TODO: Check shutdown condition, and act accordingly
            finished_jobs = _jobs_finished(job_ids=[slurm_job_id])
            if slurm_job_id in finished_jobs:
                slurm_job = jobs.pop(slurm_job_id)
                result, exception = self.process_single_task(
                    task=slurm_job.tasks[0]
                )
                if not in_compound_task:
                    if exception is None:
                        update_all_images(
                            history_item_id=history_item_id,
                            status=HistoryItemImageStatus.DONE,
                        )
                    else:
                        update_all_images(
                            history_item_id=history_item_id,
                            status=HistoryItemImageStatus.FAILED,
                        )
            time.sleep(1)
        return result, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_item_id: int,
        in_compound_task: bool = False,
        **kwargs,
    ):
        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            in_compound_task=in_compound_task,
        )

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        # TODO Pick a data structure for results and exceptions, or review the
        # interface
        results = []
        exceptions = []
        jobs: dict[str, SlurmJob] = {}

        # TODO: Add batching
        for ind, parameters in enumerate(list_parameters):
            # TODO: replace with actual values
            slurm_job = SlurmJob(
                slurm_log_file_local=f"/tmp/slurm/slurm-{ind}.log",
                slurm_log_file_remote=f"/tmp/slurm/slurm-{ind}.log",
                slurm_submission_script_local=f"/tmp/slurm/submit-{ind}.sh",
                slurm_submission_script_remote=f"/tmp/slurm/submit-{ind}.sh",
                tasks=[
                    SlurmTask(
                        input_pickle_file_local=f"/tmp/slurm/input-{ind}.pickle",
                        input_pickle_file_remote=f"/tmp/slurm/input-{ind}.pickle",
                        output_pickle_file_local=f"/tmp/slurm/output-{ind}.pickle",
                        output_pickle_file_remote=f"/tmp/slurm/output-{ind}.pickle",
                        zarr_url=parameters["zarr_url"],
                    )
                ],
            )
            slurm_job_id = self.submit_single_sbatch(
                func,
                parameters=parameters,
                slurm_job=slurm_job,
            )
            slurm_job.slurm_job_id = slurm_job_id
            jobs[slurm_job_id] = slurm_job

        # Retrieval phase
        while len(jobs) > 0:
            # TODO: Check shutdown condition, and act accordingly
            remaining_jobs = list(jobs.keys())
            finished_jobs = _jobs_finished(job_ids=remaining_jobs)
            for slurm_job_id in finished_jobs:
                slurm_job = jobs.pop(slurm_job_id)
                for task in slurm_job.tasks:
                    result, exception = self.process_single_task(task=task)
                    if not in_compound_task:
                        if exception is None:
                            update_single_image(
                                zarr_url=task.zarr_url,
                                history_item_id=history_item_id,
                                status=HistoryItemImageStatus.DONE,
                            )
                        else:
                            update_single_image(
                                zarr_url=task.zarr_url,
                                history_item_id=history_item_id,
                                status=HistoryItemImageStatus.FAILED,
                            )
                    # TODO: Now just appending, but this should be done better
                    results.append(result)
                    exceptions.append(exception)
            time.sleep(1)
        return results, exceptions
