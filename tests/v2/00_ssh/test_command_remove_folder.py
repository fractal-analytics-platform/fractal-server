import pytest

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import remove_folder_over_ssh
from fractal_server.ssh._fabric import run_command_over_ssh


logger = set_logger(__file__)


def test_unit_remove_folder_over_ssh_failures():
    # Folders which are just invalid
    invalid_folders = [
        None,
        "   /somewhere",
        "/ somewhere",
        "somewhere",
        "$(pwd)",
        "`pwd`",
    ]
    for folder in invalid_folders:
        with pytest.raises(ValueError) as e:
            remove_folder_over_ssh(
                folder=folder, safe_root="/", fractal_ssh=None
            )
        print(e.value)

    # Folders which are not relative to the accepted root
    with pytest.raises(ValueError) as e:
        remove_folder_over_ssh(folder="/", safe_root="/tmp", fractal_ssh=None)
    print(e.value)

    with pytest.raises(ValueError) as e:
        remove_folder_over_ssh(
            folder="/actual_root/../something",
            safe_root="/actual_root",
            fractal_ssh=None,
        )
    print(e.value)


def test_unit_remove_folder_over_ssh(tmp777_path, fractal_ssh):
    assert fractal_ssh.is_connected

    # Define folder
    folder = (tmp777_path / "folder").as_posix()

    # Check that folder does not exist
    with pytest.raises(ValueError) as e:
        run_command_over_ssh(cmd=f"ls {folder}", fractal_ssh=fractal_ssh)
    print(e.value)

    # Create folder
    stdout = run_command_over_ssh(
        cmd=f"mkdir -p {folder}", fractal_ssh=fractal_ssh
    )
    print(stdout)
    print()

    # Check that folder exists
    stdout = run_command_over_ssh(cmd=f"ls {folder}", fractal_ssh=fractal_ssh)
    print(stdout)
    print()

    # Remove folder
    remove_folder_over_ssh(
        folder=folder, safe_root="/tmp", fractal_ssh=fractal_ssh
    )

    # Check that folder does not exist
    with pytest.raises(ValueError) as e:
        run_command_over_ssh(cmd=f"ls {folder}", fractal_ssh=fractal_ssh)
    print(e.value)

    # Check that removing a missing folder fails
    with pytest.raises(ValueError) as e:
        remove_folder_over_ssh(
            folder="/invalid/something",
            safe_root="/invalid",
            fractal_ssh=fractal_ssh,
        )
    print(e.value)
