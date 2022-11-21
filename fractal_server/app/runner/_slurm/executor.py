import logging
import os
import shlex
import subprocess  # nosec
from pathlib import Path
from typing import List
from typing import Optional

from cfut import SlurmExecutor  # type: ignore
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject

logger = logging.getLogger("fractal")
logger.setLevel(logging.DEBUG)


def local_filename(filename=""):
    return os.path.join(os.getenv("CFUT_DIR", ".cfut"), filename)


LOG_FILE = local_filename("slurmpy.log")


def get_out_filename() -> str:
    settings = Inject(get_settings)
    script_dir = settings.RUNNER_ROOT_DIR / "slurm_backend"  # type: ignore
    return script_dir.as_posix() + "/slurmpy.stdout.{}.log"


def submit_sbatch(
    sbatch_script: str,
    submit_pre_command: str = "",
    script_dir: Optional[Path] = None,
) -> int:
    """
    Submit a Slurm job script

    Write the batch script in a temporary file and submit it with `sbatch`.

    Args:
        sbatch_script:
            the string representing the full job
        submit_pre_command:
            command that is prefixed to `sbatch`
        script_dir:
            destination of temporary script files

    Returns:
        jobid:
            integer job id as returned by `sbatch` submission
    """
    if not script_dir:
        settings = Inject(get_settings)
        script_dir = settings.RUNNER_ROOT_DIR / "slurm_backend"  # type: ignore

    filename = script_dir / f"_temp_{random_string()}.sh"
    with filename.open("w") as f:
        f.write(sbatch_script)
    submit_command = f"sbatch --parsable {filename}"
    full_cmd = shlex.join(
        shlex.split(submit_pre_command) + shlex.split(submit_command)
    )
    output = subprocess.run(full_cmd, capture_output=True, check=True)  # nosec
    logger.debug(output)
    jobid = output.stdout
    # NOTE after debugging this can be uncommented
    # filename.unlink()
    return int(jobid)


def compose_sbatch_script(
    cmdline: List[str],
    # NOTE: In SLURM, `%j` is the placeholder for the job_id.
    outpat: Optional[str] = None,
    additional_setup_lines=[],
) -> str:
    if outpat is None:
        get_out_filename().format("%j")
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --output={}".format(outpat),
        *additional_setup_lines,
        shlex.join(["srun", *cmdline]),
    ]
    return "\n".join(script_lines)


class FractalSlurmExecutor(SlurmExecutor):
    def __init__(
        self,
        username: Optional[str] = None,
        script_dir: Optional[Path] = None,
        *args,
        **kwargs,
    ):
        """
        Fractal slurm executor

        Args:
            username:
                shell username that runs the `sbatch` command
        """
        super().__init__(*args, **kwargs)
        self.username = username
        self.script_dir = script_dir

    def _start(self, workerid, additional_setup_lines):
        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines

        sbatch_script = compose_sbatch_script(
            cmdline=shlex.split(f"python3 -m cfut.remote {workerid}"),
            additional_setup_lines=additional_setup_lines,
        )

        pre_cmd = ""
        if self.username:
            pre_cmd = f"sudo --non-interactive -u {self.username}"

        job_id = submit_sbatch(
            sbatch_script,
            submit_pre_command=pre_cmd,
            script_dir=self.script_dir,
        )
        return job_id
