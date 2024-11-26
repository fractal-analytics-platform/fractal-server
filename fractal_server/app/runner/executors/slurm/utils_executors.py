from pathlib import Path
from typing import Literal
from typing import Optional


def get_pickle_file_path(
    *,
    arg: str,
    workflow_dir: Path,
    subfolder_name: str,
    in_or_out: Literal["in", "out"],
    prefix: str,
) -> Path:
    if in_or_out in ["in", "out"]:
        output = (
            workflow_dir
            / subfolder_name
            / f"{prefix}_{in_or_out}_{arg}.pickle"
        )
        return output
    else:
        raise ValueError(
            f"Missing or unexpected value in_or_out argument, {in_or_out=}"
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
    prefix: str,
) -> Path:
    if out_or_err == "out":
        return (
            workflow_dir
            / subfolder_name
            / f"{prefix}_slurm_{arg}.{out_or_err}"
        )
    elif out_or_err == "err":
        return (
            workflow_dir
            / subfolder_name
            / f"{prefix}_slurm_{arg}.{out_or_err}"
        )
    else:
        raise ValueError(
            "Missing or unexpected value out_or_err argument, "
            f"{out_or_err=}"
        )
