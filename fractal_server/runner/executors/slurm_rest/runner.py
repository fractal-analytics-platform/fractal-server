import json
from pathlib import Path
from typing import Self
from typing import override

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.logger import set_logger
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (
    SlurmJob,
)
from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import FractalSSH

from .client import SlurmRestClient
from .job_description import build_job_description
from .job_description import validate_sbatch_lines
from .script import build_rest_script
from .settings import get_slurm_rest_settings
from .sso_token import SlurmTokenError
from .token import get_token_provider

logger = set_logger(__name__)

STATUS_PLACEHOLDER = "FRACTAL_STATUS_PLACEHOLDER"
# Consecutive failed status queries before giving up
MAX_POLL_FAILURES = 20


class SlurmRestRunner(SlurmSSHRunner):
    """
    `slurm_ssh` runner where submission, polling and cancellation go through
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
    _poll_failures: int = 0
    _waiting_for_token: bool = False

    def __init__(
        self,
        *,
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: list[str] | None = None,
        resource: Resource,
        slurm_account: str | None = None,
        profile: Profile,
        user_cache_dir: str,
        fractal_ssh: FractalSSH,
        fractal_job_id: int,
        resource_id: int,
        user_id: int,
    ) -> None:
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
        # Fail at job start rather than at the first submission of a task
        validate_sbatch_lines(
            self.shared_config.default_slurm_config.extra_lines
            + (
                self.shared_config.gpu_slurm_config.extra_lines
                if self.shared_config.gpu_slurm_config is not None
                else []
            )
            + self.common_script_lines
        )
        self.rest_settings = get_slurm_rest_settings()
        token_provider = get_token_provider(
            settings=self.rest_settings,
            user_id=user_id,
            profile_username=profile.username,
        )
        self.slurm_username = token_provider.slurm_username
        # Not `printenv HOME`, which is the home of the SSH user
        passwd_entry = self.fractal_ssh.run_command(
            cmd=f"getent passwd {self.slurm_username}"
        )
        self.slurm_home = passwd_entry.strip().split(":")[5]
        # Same `PATH` that `sbatch` over SSH passes on to the SLURM job
        self.remote_path = self.fractal_ssh.run_command(
            cmd="printenv PATH"
        ).strip()
        self.rest_client = SlurmRestClient(
            base_url=self.rest_settings.FRACTAL_SLURM_REST_URL,
            api_version=self.rest_settings.FRACTAL_SLURM_REST_API_VERSION,
            username=self.slurm_username,
            get_token=token_provider.get_token,
            verify_tls=self.rest_settings.FRACTAL_SLURM_REST_VERIFY_TLS
            == "true",
            ca_bundle=self.rest_settings.FRACTAL_SLURM_REST_CA_BUNDLE,
        )
        self.rest_client.ping()
        logger.info(
            f"SLURM REST runner ready ({self.rest_client.base_url}, "
            f"ssh user={profile.username}, "
            f"slurm user={self.slurm_username}, "
            f"token source={token_provider.source})."
        )

    def _job_environment(self) -> list[str]:
        return [
            f"PATH={self.remote_path}",
            f"HOME={self.slurm_home}",
            f"USER={self.slurm_username}",
        ]

    @override
    def _mkdir_remote_folder(self: Self, folder: str) -> None:
        # Group-writable and setgid, for jobs running as the SLURM user
        self.fractal_ssh.run_command(cmd=f"mkdir -p -m 2775 {folder}")

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
            script=build_rest_script(
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
        completed upstream, as for `squeue`). Transient failures return a
        placeholder status, up to `MAX_POLL_FAILURES` consecutive failures.

        Without a valid token (e.g. SSO login expired), jobs keep the
        placeholder status until the user logs in again or the job is
        stopped: they are not treated as completed, and this does not
        count towards `MAX_POLL_FAILURES`.
        """
        if len(job_ids) == 0:
            return ""
        placeholder_output = "\n".join(
            f"{job_id} {STATUS_PLACEHOLDER}" for job_id in job_ids
        )
        try:
            states = self.rest_client.get_job_states()
            self._poll_failures = 0
        except SlurmTokenError as e:
            # TODO: notify the user, and give up after a (long) limit
            if not self._waiting_for_token:
                self._waiting_for_token = True
                logger.warning(
                    "[run_squeue] No valid SLURM token, waiting for a new "
                    f"login. Original error: {str(e)}"
                )
            return placeholder_output
        except Exception as e:
            self._poll_failures += 1
            logger.warning(
                f"[run_squeue] REST query failed ({self._poll_failures}/"
                f"{MAX_POLL_FAILURES}). Original error: {str(e)}"
            )
            if self._poll_failures >= MAX_POLL_FAILURES:
                raise
            return placeholder_output
        if self._waiting_for_token:
            self._waiting_for_token = False
            logger.info("[run_squeue] Valid SLURM token again.")
        return "\n".join(
            f"{job_id} {states[job_id]}"
            for job_id in job_ids
            if job_id in states
        )

    @override
    def scancel_jobs(self) -> list[str]:
        logger.info("[scancel_jobs] START (REST)")
        scancelled_job_ids = self.job_ids
        logger.info(f"[scancel_jobs] {len(scancelled_job_ids)=}")
        for job_id in scancelled_job_ids:
            try:
                self.rest_client.cancel_job(job_id)
                logger.warning(f"[scancel_jobs] Cancelled {job_id}.")
            except Exception as e:
                logger.error(
                    f"[scancel_jobs] Cancel of {job_id} failed. "
                    f"Original error:\n{str(e)}"
                )
        logger.info("[scancel_jobs] END (REST)")
        return scancelled_job_ids
