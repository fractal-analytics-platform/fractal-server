import json
import shlex
import time
from pathlib import Path
from typing import Self
from typing import override

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.logger import set_logger
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (
    SlurmJob,
)
from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHCommandError
from fractal_server.syringe import Inject

from .client import SlurmRestClient
from .client import SlurmRestError
from .job_description import build_job_description
from .job_description import validate_sbatch_lines
from .job_script import build_job_script
from .settings import get_slurm_rest_settings
from .token import SlurmTokenError
from .token import get_token_provider

logger = set_logger(__name__)

# Not a SLURM state (so jobs stay active), same as in
# `SlurmSSHRunner.run_squeue` when `squeue` cannot be run
STATUS_PLACEHOLDER = "FRACTAL_STATUS_PLACEHOLDER"


class SlurmRestRunner(SlurmSSHRunner):
    """
    Slurm runner where submission, polling and cancellation go through
    `slurmrestd`.

    SSH is still used for: connection/version checks, `printenv PATH`,
    `getent passwd`, remote `mkdir`, and fetching artifacts
    (`_fetch_artifacts`).

    SSH runs as `Profile.username`, while SLURM jobs run as the token user
    (`slurm_username`, the `sub` of the SSO token of the Fractal user),
    which may differ (e.g. with a shared SSH service account): both users
    must share the remote job folders.
    """

    rest_client: SlurmRestClient
    slurm_home: str
    remote_path: str
    slurm_username: str
    # Why job states cannot be queried, if they cannot (for logs)
    _poll_problem: str | None = None
    # Whether the last cancellation failed (for logs and retries)
    _cancel_failed: bool = False

    def __init__(
        self,
        *,
        # Common
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: list[str] | None = None,
        resource: Resource,
        # Specific
        slurm_account: str | None = None,
        profile: Profile,
        user_cache_dir: str,
        fractal_ssh: FractalSSH,
        fractal_job_id: int,
        resource_id: int,
        user_id: int,
    ) -> None:
        # Checks that need no SSH come first, if they fail (e.g. expired SSO
        # login, `slurmrestd` down), no SSH command is run and no remote job
        # folder is created.
        shared_config = JobRunnerConfigSLURM(**resource.jobs_runner_config)
        # Invalid `#SBATCH` lines fail here, not at task submission.
        validate_sbatch_lines(
            shared_config.default_slurm_config.extra_lines
            + (
                shared_config.gpu_slurm_config.extra_lines
                if shared_config.gpu_slurm_config is not None
                else []
            )
            + (common_script_lines or [])
        )
        self._init_rest_client(
            user_id=user_id, profile_username=profile.username
        )

        super().__init__(
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            resource=resource,
            slurm_account=slurm_account,
            profile=profile,
            user_cache_dir=user_cache_dir,
            fractal_ssh=fractal_ssh,
            fractal_job_id=fractal_job_id,
            resource_id=resource_id,
        )
        # After `super().__init__`, which checks the SSH connection
        self.slurm_home = self._get_slurm_home()
        # Same `PATH` that `sbatch` over SSH passes on to the SLURM job
        self.remote_path = self.fractal_ssh.run_command(
            cmd="printenv PATH"
        ).strip()
        logger.info(
            f"SLURM REST runner ready ({self.rest_client.base_url}, "
            f"ssh user={profile.username}, "
            f"slurm user={self.slurm_username})."
        )

    def _init_rest_client(self, *, user_id: int, profile_username: str) -> None:
        """
        Set the SLURM user and the REST client, and check that `slurmrestd`
        is reachable (before any submission).

        Token errors (e.g. no SSO account, expired login) and `slurmrestd`
        errors become job errors with a hint, instead of tracebacks.
        """
        rest_settings = Inject(get_slurm_rest_settings)
        try:
            token_provider = get_token_provider(
                settings=rest_settings,
                user_id=user_id,
                profile_username=profile_username,
            )
            self.slurm_username = token_provider.slurm_username
            self.rest_client = SlurmRestClient(
                base_url=rest_settings.base_url,
                username=self.slurm_username,
                get_token=token_provider.get_token,
                ca_bundle=rest_settings.FRACTAL_SLURM_REST_CA_BUNDLE,
            )
            self.rest_client.ping()
        except SlurmTokenError as e:
            raise JobExecutionError(info=str(e)) from None
        except SlurmRestError as e:
            raise JobExecutionError(
                info=(
                    f"Cannot reach slurmrestd at {self.rest_client.base_url} "
                    f"({e}). Hint: check that slurmrestd "
                    "is running and reachable, FRACTAL_SLURM_REST_URL, and "
                    "the trusted CAs (FRACTAL_SLURM_REST_CA_BUNDLE)."
                )
            ) from None

    def _get_slurm_home(self) -> str:
        """
        Home folder of the SLURM user, from `getent passwd` (not
        `printenv HOME`, which is the home of the SSH user).
        """
        cmd = f"getent passwd {shlex.quote(self.slurm_username)}"
        try:
            return self.fractal_ssh.run_command(cmd=cmd).split(":")[5].strip()
        except (FractalSSHCommandError, IndexError):
            raise JobExecutionError(
                info=(
                    f"SLURM user '{self.slurm_username}' is unknown on the "
                    "cluster (`getent passwd` failed). Hint: the SSO user "
                    "name must be a UNIX user of the cluster."
                )
            ) from None

    def _job_environment(self) -> list[str]:
        return [
            f"PATH={self.remote_path}",
            f"HOME={self.slurm_home}",
            f"USER={self.slurm_username}",
        ]

    @override
    def _mkdir_remote_folder(self: Self, folder: str) -> None:
        # Group-writable and setgid, for jobs running as the SLURM user
        self.fractal_ssh.run_command(
            cmd=f"mkdir -p -m 2775 {shlex.quote(folder)}"
        )

    @override
    def _send_many_job_inputs(
        self, *, workdir_local: Path, workdir_remote: Path
    ) -> None:
        # Input files are embedded in the submission script instead.
        logger.debug("[_send_many_job_inputs] Skip (REST runner).")

    @override
    def _submit_single_sbatch(
        self,
        *,
        submit_command: str,
        slurm_job: SlurmJob,
    ) -> None:
        """
        Submit `slurm_job` via `POST /job/submit`.

        `submit_command` (an `sbatch` command line) is ignored: the script
        written by `_prepare_single_slurm_job` is read back and translated.
        """
        logger.debug("[_submit_single_sbatch] START (REST)")

        script_local_path = Path(slurm_job.slurm_submission_script_local)
        original_script = script_local_path.read_text()

        files = []
        for task in slurm_job.tasks:
            files.append(
                (
                    task.input_file_remote,
                    Path(task.input_file_local).read_text(),
                )
            )
            files.append(
                (
                    task.task_files.args_file_remote,
                    Path(task.task_files.args_file_local).read_text(),
                )
            )

        job_description = build_job_description(
            script=original_script,
            environment=self._job_environment(),
        )
        request_body = dict(
            script=build_job_script(
                script=original_script,
                files=files,
                script_copy_path=slurm_job.slurm_submission_script_remote,
            ),
            job=job_description,
        )
        request_path = script_local_path.with_suffix(".json")
        request_path.write_text(json.dumps(request_body, indent=2))
        logger.debug(f"[_submit_single_sbatch] Written {request_path}.")

        submitted_job_id = self.rest_client.submit_job(request_body)
        logger.info(f"[_submit_single_sbatch] {submitted_job_id=}")
        slurm_job.slurm_job_id = str(submitted_job_id)
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug("[_submit_single_sbatch] END (REST)")

    @override
    def run_squeue(self: Self, *, job_ids: list[str]) -> str:
        """
        Mock `squeue --format='%i %T'` output, based on `GET /jobs`.

        Jobs missing from the response are omitted (and then treated as
        completed upstream, as for `squeue`).

        If job states cannot be queried (no valid token, e.g. expired SSO
        login, or `slurmrestd` unreachable or failing), all jobs get a
        placeholder status and stay active, and the query is repeated at the
        next poll, with no limit. A failed query never marks jobs as
        completed: that would fetch their outputs too early and leave the
        SLURM jobs running untracked.
        """
        if len(job_ids) == 0:
            return ""
        try:
            states = self.rest_client.get_job_states()
        except Exception as e:
            # TODO: notify the user, and give up after a (long) limit
            if isinstance(e, SlurmTokenError):
                problem = "No valid SLURM token, waiting for a new login"
            else:
                problem = "Cannot query SLURM job states, retrying"
            # Logged once per problem, not at every poll
            if problem != self._poll_problem:
                self._poll_problem = problem
                logger.warning(
                    f"[run_squeue] {problem}. Original error: {str(e)}"
                )
            return "\n".join(
                f"{job_id} {STATUS_PLACEHOLDER}" for job_id in job_ids
            )
        if self._poll_problem is not None:
            self._poll_problem = None
            logger.info("[run_squeue] SLURM job states available again.")
        return "\n".join(
            f"{job_id} {states[job_id]}"
            for job_id in job_ids
            if job_id in states
        )

    @override
    def scancel_jobs(self) -> list[str]:
        """
        Cancel all SLURM jobs via `DELETE /job/{id}`.

        While cancellation keeps failing (after a shutdown, it is retried at
        each poll, see `wait_and_check_shutdown`), only the first failure is
        logged above debug level, as for `run_squeue`.
        """
        log = logger.debug if self._cancel_failed else logger.info
        log("[scancel_jobs] START (REST)")
        scancelled_job_ids = self.job_ids
        log(f"[scancel_jobs] {len(scancelled_job_ids)=}")
        errors = {}
        for job_id in scancelled_job_ids:
            try:
                self.rest_client.cancel_job(job_id)
                logger.warning(f"[scancel_jobs] Cancelled {job_id}.")
            except Exception as e:
                errors[job_id] = str(e)
        if errors:
            log_error = logger.debug if self._cancel_failed else logger.error
            log_error(
                f"[scancel_jobs] Cancel failed. Original errors: {errors}"
            )
            self._cancel_failed = True
        elif self._cancel_failed:
            self._cancel_failed = False
            logger.info("[scancel_jobs] Cancel possible again.")
        log("[scancel_jobs] END (REST)")
        return scancelled_job_ids

    @override
    def wait_and_check_shutdown(self) -> list[str]:
        """
        Same as `SlurmSSHRunner`, but wait `poll_interval` after the
        cancellation of a shutdown, and retry a failed cancellation without
        the upstream logs.

        Upstream returns right after `scancel_jobs`, expecting the SLURM jobs
        to be finished at the next poll. With REST, both the cancellation
        and the poll may fail (no valid token, `slurmrestd` down), and jobs
        then stay active: without waiting, cancellation and polling would be
        retried in a busy loop until the problem ends.
        """
        if self._cancel_failed and self.is_shutdown():
            scancelled_job_ids = self.scancel_jobs()
        else:
            scancelled_job_ids = super().wait_and_check_shutdown()
        if scancelled_job_ids:
            time.sleep(self.poll_interval)
        return scancelled_job_ids
