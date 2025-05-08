from pathlib import Path


def get_tar_compression_cmd(
    subfolder_path: Path,
    filelist_path: Path | None,
) -> tuple[str, str]:
    """
    Compress e.g. `/path/archive` into `/path/archive.tar.gz`

    Note that `/path/archive.tar.gz` may already exist. In this case, it will
    be overwritten.

    Args:
        subfolder_path: Absolute path to the folder to compress.
        remote_to_local: If `True`, exclude some files from the tar.gz archive.

    Returns:
        Absolute path to the tar.gz archive.
    """
    subfolder_name = subfolder_path.name
    tarfile_path = (
        subfolder_path.parent / f"{subfolder_name}.tar.gz"
    ).as_posix()

    if filelist_path is None:
        cmd_tar = (
            f"tar -c -z "
            f"-f {tarfile_path} "
            f"--directory={subfolder_path.as_posix()} "
            "."
        )
    else:
        cmd_tar = (
            f"tar -c -z -f {tarfile_path} "
            f"--directory={subfolder_path.as_posix()} "
            f"--files-from={filelist_path.as_posix()} --ignore-failed-read"
        )

    return cmd_tar, tarfile_path


def _remove_suffix(*, string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        raise ValueError(f"Cannot remove {suffix=} from {string=}.")


def get_tar_extraction_cmd(archive_path: Path) -> tuple[str, str]:

    # Prepare subfolder path
    parent_dir = archive_path.parent
    subfolder_name = _remove_suffix(string=archive_path.name, suffix=".tar.gz")
    subfolder_path = parent_dir / subfolder_name

    cmd_tar = (
        f"tar -xzvf {archive_path} --directory={subfolder_path.as_posix()}"
    )
    return subfolder_path.as_posix(), cmd_tar
