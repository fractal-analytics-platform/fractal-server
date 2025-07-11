import pytest

from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.ssh._fabric import FractalSSHTimeoutError
from fractal_server.ssh._fabric import FractalSSHUnknownError


def test_unit_FractalSSHList():
    credentials_A = dict(host="host", user="userA", key_path="/some/A")
    credentials_B = dict(host="host", user="userB", key_path="/some/B")

    # Create empty collection
    collection = FractalSSHList()
    assert collection.size == 0
    assert not collection._lock.locked()

    # Add a value to the collection for the first time
    fractal_ssh_A_first = collection.get(**credentials_A)
    first_id_object_A = id(fractal_ssh_A_first)
    assert collection.contains(**credentials_A)
    assert not collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()

    # Re-add the same value to the collection
    fractal_ssh_A_second = collection.get(**credentials_A)
    second_id_object_A = id(fractal_ssh_A_second)
    assert collection.contains(**credentials_A)
    assert not collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()

    # Calling `get` twice returns the same Python object
    assert first_id_object_A == second_id_object_A

    # Add a second value to the collection
    fractal_ssh_B = collection.get(**credentials_B)
    assert collection.contains(**credentials_A)
    assert collection.contains(**credentials_B)
    assert collection.size == 2
    assert not collection._lock.locked()
    assert id(fractal_ssh_B) != first_id_object_A

    # Remove a value from the collection
    collection.remove(**credentials_A)
    assert not collection.contains(**credentials_A)
    assert collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()

    # Remove a missing value from the collection
    with pytest.raises(KeyError):
        collection.remove(**credentials_A)
    assert not collection.contains(**credentials_A)
    assert collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()

    # Call `close_all`, in the presence of both valid and invalid connections
    collection.close_all()


@pytest.mark.container
@pytest.mark.ssh
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
    with pytest.raises(FractalSSHUnknownError):
        # NOTE: on Ubuntu22 this is a ValueError,
        # on MacOS this is a paramiko.ssh_exception.AuthenticationException
        invalid_fractal_ssh.run_command(cmd="ls")

    # Check that `close_all` works both for valid and invalid connections
    fractal_ssh_list.close_all()


def test_lock_FractalSSHList():
    # Create empty collection
    collection = FractalSSHList(timeout=0.1)
    assert not collection._lock.locked()

    # When lock is taken, observe timeout error
    collection._lock.acquire()
    with pytest.raises(FractalSSHTimeoutError):
        collection.get(host="host", user="user", key_path="/key_path")

    # After lock is released, the same operation goes through
    collection._lock.release()
    assert not collection._lock.locked()
    collection.get(host="host", user="user", key_path="/key_path")
