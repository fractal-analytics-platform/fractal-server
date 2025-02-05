import pytest

from fractal_server.ssh._fabric import FractalSSHList


def test_run_command_through_FractalSSHList(
    slurmlogin_ip,
    ssh_keys,
    fractal_ssh_list: FractalSSHList,
):
    valid_credentials = dict(
        host=slurmlogin_ip,
        user="fractal",
        key_path=ssh_keys["private"],
    )
    invalid_credentials = dict(
        host=slurmlogin_ip,
        user="invalid",
        key_path=ssh_keys["private"],
    )

    # Check that the fixture-generated collection already contains
    # the valid FractalSSH object, but not the invalid one
    assert fractal_ssh_list.contains(**valid_credentials)
    assert not fractal_ssh_list.contains(**invalid_credentials)

    # Fetch and use the valid `FractalSSH` object
    valid_fractal_ssh = fractal_ssh_list.get(**valid_credentials)
    stdout = valid_fractal_ssh.run_command(cmd="whoami")
    assert stdout.strip("\n") == "fractal"

    # Add the invalid `FractalSSH` object
    invalid_fractal_ssh = fractal_ssh_list.get(**invalid_credentials)
    assert fractal_ssh_list.contains(**invalid_credentials)

    # Try using the invalid `FractalSSH` object
    with pytest.raises(
        ValueError,
    ):
        invalid_fractal_ssh.run_command(cmd="ls")

    # Check that `close_all` works both for valid and invalid connections
    fractal_ssh_list.close_all()
