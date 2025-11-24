"""
Prepare tar commands for task-subfolder compression/extraction.
"""

from pathlib import Path


def get_tar_compression_cmd(
    subfolder_path: Path,
    filelist_path: Path | None,
) -> str:
    """
    Prepare command to compress e.g. `/path/dir` into `/path/dir.tar.gz`.

    Note that `/path/dir.tar.gz` may already exist. In this case, it will
    be overwritten.

    Args:
        subfolder_path: Absolute path to the folder to compress.
        filelist_path: If set, to be used in the `--files-from` option.

    Returns:
        tar command
    """
    tarfile_path = subfolder_path.with_suffix(".tar.gz")
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

    return cmd_tar


def _check_suffix(archive_path: Path):
    """
    FIXME
    """
    if archive_path.suffixes[-2:] != [".tar", ".gz"]:
        raise ValueError(
            "Archive path must end with `.tar.gz` "
            f"(given: {archive_path.as_posix()})"
        )


def get_tar_extraction_cmd(archive_path: Path) -> tuple[Path, str]:
    """
    Prepare command to extract e.g. `/path/dir.tar.gz` into `/path/dir`.

    Args:
        archive_path: Absolute path to the tar.gz archive.

    Returns:
        Target extraction folder and tar command
    """

    # Prepare subfolder path
    _check_suffix(archive_path)
    subfolder_path = archive_path.with_suffix("").with_suffix("")

    cmd_tar = (
        f"tar -xzvf {archive_path} --directory={subfolder_path.as_posix()}"
    )
    return subfolder_path, cmd_tar


def get_tar_extraction_cmd_with_target_folder(
    archive_path: Path,
    target_folder: str,
) -> str:
    """
    Prepare command to extract e.g. `/path/dir.tar.gz` into `/path/dir`.

    Args:
        archive_path: Absolute path to the tar.gz archive.
        target_folder:
            Absolute path of folder where archive files should be extracted.

    Returns:
        tar command
    """

    # Prepare subfolder path
    _check_suffix(archive_path)

    cmd_tar = f"tar -xzvf {archive_path} --directory={target_folder.as_posix()}"
    return cmd_tar


def get_tar_compression_cmd_with_target(
    archive_file: Path,
    filelist_path: Path | None,
) -> str:
    """
    FIXME
    """
    _check_suffix(archive_file)
    cmd_tar = (
        f"tar -c -z -f {archive_file} "
        f"--directory={filelist_path.parent.as_posix()} "
        f"--files-from={filelist_path.as_posix()} --ignore-failed-read"
    )

    return cmd_tar
