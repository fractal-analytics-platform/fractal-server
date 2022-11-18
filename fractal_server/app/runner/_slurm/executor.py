import os
import shlex
import sys
from typing import List
from typing import Optional

from cfut import SlurmExecutor  # type: ignore
from cfut.util import chcall  # type: ignore
from cfut.util import random_string


def local_filename(filename=""):
    return os.path.join(os.getenv("CFUT_DIR", ".cfut"), filename)


LOG_FILE = local_filename("slurmpy.log")
OUTFILE_FMT = local_filename("slurmpy.stdout.{}.log")


def submit_sbatch(sbatch_script: str, submit_pre_command: str = "") -> int:
    """Submits a Slurm job represented as a job file string. Returns
    the job ID.
    """
    filename = local_filename("_temp_{}.sh".format(random_string()))
    with open(filename, "w") as f:
        f.write(sbatch_script)
    submit_command = f"sbatch --parsable {filename}"
    jobid, _ = chcall(
        shlex.join(
            shlex.split(submit_pre_command) + shlex.split(submit_command)
        )
    )
    os.unlink(filename)
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
    def __init__(self, user: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _start(self, workerid, additional_setup_lines):
        if additional_setup_lines is None:
            additional_setup_lines = self.additional_setup_lines

        sbatch_script = compose_sbatch_script(
            cmdline=f"{sys.executable} -m cfut.remote {workerid}",
            additional_setup_lines=additional_setup_lines,
        )
        job_id = submit_sbatch(sbatch_script)
        return job_id
