from uuid import uuid4

from .job_description import SBATCH_PREFIX

# Default `slurmctld` limit (SchedulerParameters=max_script_size)
MAX_SCRIPT_BYTES = 4 * 1024 * 1024


def build_job_script(
    *,
    script: str,
    files: list[tuple[str, str]],
    script_copy_path: str | None = None,
) -> str:
    """
    Build the `script` of a `POST /job/submit` request (the `job` part comes
    from `job_description.py`), embedding the input files.

    The REST runner has no SSH upload step: the files that `remote.py` needs
    (input and args JSON files) are written by the job itself via quoted
    heredocs, right after the shebang/`#SBATCH` header. The job uses
    `umask 0002`, so that its files are group-writable and readable by the
    SSH user.

    Args:
        script: Original submission script.
        files: List of `(remote_path, content)` pairs.
        script_copy_path:
            If set, the job copies its own script there (as the SSH runner
            would have uploaded it).
    """
    lines = script.splitlines()
    header_end = 0
    for ind, line in enumerate(lines):
        if (ind == 0 and line.startswith("#!")) or line.startswith(
            SBATCH_PREFIX
        ):
            header_end = ind + 1
        else:
            break

    paths = [path for path, _ in files] + [script_copy_path or ""]
    if any("'" in path for path in paths):
        raise ValueError(f"Single quotes are not allowed in paths: {paths}.")

    block = ["umask 0002"]
    for remote_path, content in files:
        delimiter = f"FRACTAL_EOF_{uuid4().hex}"
        if delimiter in content:
            raise ValueError(f"Heredoc delimiter found in {remote_path}.")
        block.append(f"cat > '{remote_path}' <<'{delimiter}'")
        block.append(content.rstrip("\n"))
        block.append(delimiter)
    if script_copy_path is not None:
        block.append(f"cp \"$0\" '{script_copy_path}'")

    new_script = "\n".join(lines[:header_end] + block + lines[header_end:])
    new_script += "\n"
    size = len(new_script.encode())
    if size > MAX_SCRIPT_BYTES:
        raise ValueError(
            f"Submission script is {size} bytes, above the "
            f"{MAX_SCRIPT_BYTES}-bytes limit."
        )
    return new_script
