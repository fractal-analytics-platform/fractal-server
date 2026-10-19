import json
import re
from pathlib import Path
from typing import ClassVar
from typing import Self
from typing import override

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.logger import set_logger
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.slurm_common.fractal_slurm_jobs import (
    FractalSlurmJob,
)
from fractal_server.runner.executors.slurm_common.fractal_slurm_jobs import (
    is_active_state,
)
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (
    SlurmJob,
)
from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import FractalSSH

from .client import SlurmRestClient
from .client import SlurmRestError
from .job_description import build_job_description
from .job_description import validate_sbatch_lines
from .jobs import collect_fractal_slurm_jobs
from .script import build_rest_script
from .settings import get_slurm_rest_settings
from .token import get_token_provider

logger = set_logger(__name__)

STATUS_PLACEHOLDER = "FRACTAL_STATUS_PLACEHOLDER"
# Consecutive failed status queries before giving up
MAX_POLL_FAILURES = 20
HEREDOC_DELIMITER = re.compile(r"FRACTAL_EOF_[0-9a-f]{32}")


def normalize_script(script: str) -> str:
    # Heredoc delimiters are random, see `build_rest_script`
    return HEREDOC_DELIMITER.sub("FRACTAL_EOF", script)


def build_rest_client(
    *, fractal_ssh: FractalSSH, user_id: int, username: str
) -> SlurmRestClient:
    """
    REST client for `username`, with tokens from `get_token_provider`.
    """
    settings = get_slurm_rest_settings()
    token_provider = get_token_provider(
        settings=settings,
        fractal_ssh=fractal_ssh,
        user_id=user_id,
        slurm_username=username,
    )
    return SlurmRestClient(
        base_url=settings.FRACTAL_SLURM_REST_URL,
        api_version=settings.FRACTAL_SLURM_REST_API_VERSION,
        username=username,
        get_token=token_provider.get_token,
        verify_tls=settings.FRACTAL_SLURM_REST_VERIFY_TLS == "true",
        ca_bundle=settings.FRACTAL_SLURM_REST_CA_BUNDLE,
    )


class SlurmRestRunner(SlurmSSHRunner):
    """
    `slurm_ssh` runner where submission, polling and cancellation go through
    `slurmrestd`.

    SSH is still used for: connection/version checks, remote `mkdir`,
    `scontrol token` (if `FRACTAL_SLURM_REST_TOKEN_SOURCE=scontrol`), and
    fetching artifacts (`_fetch_artifacts`).

    In adopt mode (used to resume an orphaned fractal job), a SLURM job
    with the same subfolder and prefix (from its stdout path) as the one
    about to be submitted is taken over instead, if it was submitted with
    the same script.
    """

    rest_client: SlurmRestClient
    remote_home: str
    remote_path: str
    remote_username: str
    adopt: bool = False
    supports_resume: ClassVar[bool] = True
    _poll_failures: int = 0

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
        adopt_since: int | None = None,
    ) -> None:
        """
        Args:
            adopt_since:
                If set, enable adopt mode for SLURM jobs started after this
                Unix timestamp.
        """
        self.adopt = adopt_since is not None
        self._adoptable: dict[tuple[str, str], FractalSlurmJob] = {}
        self._adopted_ids: set[str] = set()
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
        self.remote_username = profile.username
        self.remote_home = self.fractal_ssh.run_command(
            cmd="printenv HOME"
        ).strip()
        # Same `PATH` that `sbatch` over SSH passes on to the SLURM job
        self.remote_path = self.fractal_ssh.run_command(
            cmd="printenv PATH"
        ).strip()
        self.rest_client = build_rest_client(
            fractal_ssh=self.fractal_ssh,
            user_id=user_id,
            username=self.remote_username,
        )
        self.rest_client.ping()
        if self.adopt:
            # Sorted by SLURM job ID, so the most recent one wins
            for found in collect_fractal_slurm_jobs(
                client=self.rest_client,
                job_root=root_dir_remote.as_posix(),
                start_time=adopt_since,
            ):
                self._adoptable[(found.subfolder, found.prefix)] = found
            logger.info(f"Adopt mode: {len(self._adoptable)} SLURM jobs found.")
        logger.info(
            f"SLURM REST runner ready ({self.rest_client.base_url}, "
            f"user={self.remote_username})."
        )

    @classmethod
    @override
    def list_fractal_jobs(
        cls,
        *,
        resource: Resource,
        profile: Profile,
        fractal_ssh: FractalSSH,
        user_id: int,
        job_root: str,
        since: int,
    ) -> list[FractalSlurmJob]:
        return collect_fractal_slurm_jobs(
            client=build_rest_client(
                fractal_ssh=fractal_ssh,
                user_id=user_id,
                username=profile.username,
            ),
            job_root=job_root,
            start_time=since,
        )

    def _job_environment(self) -> list[str]:
        return [
            f"PATH={self.remote_path}",
            f"HOME={self.remote_home}",
            f"USER={self.remote_username}",
        ]

    @override
    def _mkdir_local_folder(self: Self, folder: str) -> None:
        # In adopt mode, the folder of the interrupted task already exists
        Path(folder).mkdir(parents=True, exist_ok=self.adopt)

    @property
    @override
    def job_ids_int(self) -> list[int]:
        # Only used for `AccountingRecordSlurm`; adopted SLURM jobs were
        # already accounted for by the interrupted run
        return [
            int(job_id)
            for job_id in self.jobs
            if job_id not in self._adopted_ids
        ]

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
        if self.adopt:
            found = self._adoptable.pop(
                (slurm_job.workdir_remote.name, slurm_job.prefix), None
            )
            if found is not None and self._adopt(
                slurm_job=slurm_job,
                found=found,
                previous_script=self._previous_script(
                    slurm_job=slurm_job, request_path=request_path
                ),
                script=request_body["script"],
            ):
                return
        request_path.write_text(json.dumps(request_body, indent=2))
        logger.debug(f"[_submit_single_sbatch] Written {request_path}.")

        submitted_job_id = self.rest_client.submit_job(request_body)
        logger.info(f"[_submit_single_sbatch] {submitted_job_id=}")
        slurm_job.slurm_job_id = str(submitted_job_id)
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug("[_submit_single_sbatch] END (REST)")

    def _previous_script(
        self, *, slurm_job: SlurmJob, request_path: Path
    ) -> str | None:
        """
        Script of the earlier submission, from the local request file or
        from the remote copy (which only exists once the job started).
        """
        if request_path.exists():
            return json.loads(request_path.read_text())["script"]
        remote_copy = slurm_job.slurm_submission_script_remote
        if self.fractal_ssh.remote_exists(remote_copy):
            return self.fractal_ssh.read_remote_text_file(remote_copy)
        return None

    def _adopt(
        self,
        *,
        slurm_job: SlurmJob,
        found: FractalSlurmJob,
        previous_script: str | None,
        script: str,
    ) -> bool:
        """
        Take over `found` for `slurm_job`.

        Returns:
            Whether the SLURM job was adopted; if not, a new one must be
            submitted.
        """
        if previous_script is None:
            logger.warning(
                f"[_adopt] Cannot verify SLURM job {found.slurm_job_id} "
                f"({found.state}), submit a new one."
            )
            if found.is_active:
                try:
                    self.rest_client.cancel_job(found.slurm_job_id)
                except SlurmRestError:
                    # Fine if it finished meanwhile (missing = finished)
                    state = self.rest_client.get_job_states().get(
                        found.slurm_job_id, "COMPLETED"
                    )
                    if is_active_state(state):
                        raise
                    logger.info(
                        f"[_adopt] SLURM job {found.slurm_job_id} already "
                        f"finished ({state})."
                    )
            return False
        if normalize_script(previous_script) != normalize_script(script):
            raise JobExecutionError(
                f"SLURM job {found.slurm_job_id} was submitted with a "
                "different script, it cannot be adopted."
            )
        logger.info(
            f"[_adopt] Adopt SLURM job {found.slurm_job_id} ({found.state})."
        )
        slurm_job.slurm_job_id = found.slurm_job_id
        self.jobs[found.slurm_job_id] = slurm_job
        self._adopted_ids.add(found.slurm_job_id)
        return True

    @override
    def run_squeue(self: Self, *, job_ids: list[str]) -> str:
        """
        Mock `squeue --format='%i %T'` output, based on `GET /jobs`.

        Jobs missing from the response are omitted (and then treated as
        completed upstream, as for `squeue`). Transient failures return a
        placeholder status, up to `MAX_POLL_FAILURES` consecutive failures.
        """
        if len(job_ids) == 0:
            return ""
        try:
            states = self.rest_client.get_job_states()
            self._poll_failures = 0
        except Exception as e:
            self._poll_failures += 1
            logger.warning(
                f"[run_squeue] REST query failed ({self._poll_failures}/"
                f"{MAX_POLL_FAILURES}). Original error: {str(e)}"
            )
            if self._poll_failures >= MAX_POLL_FAILURES:
                raise
            return "\n".join(
                f"{job_id} {STATUS_PLACEHOLDER}" for job_id in job_ids
            )
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
