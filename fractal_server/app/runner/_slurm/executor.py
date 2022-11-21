import os
import shlex
import sys
from pathlib import Path
from typing import List
from typing import Optional

from cfut import SlurmExecutor  # type: ignore
from cfut.util import chcall  # type: ignore
from cfut.util import random_string

from ....config import get_settings
from ....syringe import Inject


def local_filename(filename=""):
    return os.path.join(os.getenv("CFUT_DIR", ".cfut"), filename)


LOG_FILE = local_filename("slurmpy.log")
OUTFILE_FMT = local_filename("slurmpy.stdout.{}.log")


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
    jobid, _ = chcall(
        shlex.join(
            shlex.split(submit_pre_command) + shlex.split(submit_command)
        )
    )
    filename.unlink()
    return int(jobid)


def compose_sbatch_script(
    cmdline: List[str],
    # NOTE: In SLURM, `%j` is the placeholder for the job_id.
    outpat: str = OUTFILE_FMT.format("%j"),
    additional_setup_lines=[],
) -> str:
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
            cmdline=shlex.split(f"{sys.executable} -m cfut.remote {workerid}"),
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
