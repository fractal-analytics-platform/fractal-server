import pytest
from fabric import Connection

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH

logger = set_logger(__file__)


def test_unit_remove_folder_over_ssh_failures():
    fake_fractal_ssh = FractalSSH(connection=Connection(host="localhost"))

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
            fake_fractal_ssh.remove_folder(folder=folder, safe_root="/")
        print(e.value)

    # Folders which are just invalid
    invalid_folders = [
        None,
        "   /somewhere",
        "/ somewhere",
        "somewhere",
        "$(pwd)",
        "`pwd`",
    ]
    for safe_root in invalid_folders:
        with pytest.raises(ValueError) as e:
            fake_fractal_ssh.remove_folder(
                folder="/tmp/something",
                safe_root=safe_root,
            )
        print(e.value)

    # Folders which are not relative to the accepted root
    with pytest.raises(ValueError) as e:
        fake_fractal_ssh.remove_folder(folder="/", safe_root="/tmp")
    print(e.value)

    with pytest.raises(ValueError) as e:
        fake_fractal_ssh.remove_folder(
            folder="/actual_root/../something",
            safe_root="/actual_root",
        )
    print(e.value)


def test_remove_folder_over_ssh(tmp777_path, fractal_ssh: FractalSSH):
    assert fractal_ssh.is_connected

    # Define folder
    folder = (tmp777_path / "nested/folder").as_posix()

    # Check that folder does not exist
    with pytest.raises(ValueError) as e:
        fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(e.value)

    # Try to create folder, without parents options
    with pytest.raises(ValueError) as e:
        fractal_ssh.mkdir(folder=folder, parents=False)
    print(e.value)

    # Create folder
    fractal_ssh.mkdir(folder=folder, parents=True)

    # Check that folder exists
    stdout = fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(stdout)
    print()

    # Remove folder
    fractal_ssh.remove_folder(folder=folder, safe_root="/tmp")

    # Check that folder does not exist
    with pytest.raises(ValueError) as e:
        fractal_ssh.run_command_over_ssh(cmd=f"ls {folder}")
    print(e.value)

    # Check that removing a missing folder fails
    with pytest.raises(ValueError) as e:
        fractal_ssh.remove_folder(
            folder="/invalid/something",
            safe_root="/invalid",
        )
    print(e.value)
