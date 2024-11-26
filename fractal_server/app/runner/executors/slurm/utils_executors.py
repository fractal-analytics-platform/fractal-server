from pathlib import Path
from typing import Literal
from typing import Optional

from ...task_files import TaskFiles


def get_default_task_files(
    *, workflow_dir_local: Path, workflow_dir_remote: Path
) -> TaskFiles:
    """
    This will be called when self.submit or self.map are called from
    outside fractal-server, and then lack some optional arguments.
    """
    task_files = TaskFiles(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=None,
        task_name="name",
    )
    return task_files


def get_pickle_file_path(
    *,
    arg: str,
    workflow_dir: Path,
    subfolder_name: str,
    in_or_out: Literal["in", "out"],
    prefix: Optional[str] = None,
) -> Path:
    prefix = prefix or "cfut"
    if in_or_out in ["in", "out"]:
        output = (
            workflow_dir
            / subfolder_name
            / f"{prefix}_{in_or_out}_{arg}.pickle"
        )
        return output
    else:
        raise ValueError(
            "Missing or unexpected value in_or_out argument, " f"{in_or_out=}"
        )


def get_slurm_script_file_path(
    *, workflow_dir: Path, subfolder_name: str, prefix: Optional[str] = None
) -> Path:
    prefix = prefix or "_temp"
    return workflow_dir / subfolder_name / f"{prefix}_slurm_submit.sbatch"


def get_slurm_file_path(
    *,
    workflow_dir: Path,
    subfolder_name: str,
    arg: str = "%j",
    out_or_err: Literal["out", "err"],
    prefix: Optional[str] = None,
) -> Path:
    if out_or_err == "out":
        prefix = prefix or "slurmpy.stdout"
        return workflow_dir / subfolder_name / f"{prefix}_slurm_{arg}.out"
    elif out_or_err == "err":
        prefix = prefix or "slurmpy.stderr"
        return workflow_dir / subfolder_name / f"{prefix}_slurm_{arg}.err"
    else:
        raise ValueError(
            "Missing or unexpected value out_or_err argument, "
            f"{out_or_err=}"
        )
