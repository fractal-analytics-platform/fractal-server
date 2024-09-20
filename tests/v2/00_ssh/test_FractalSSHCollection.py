import pytest

from fractal_server.ssh._fabric import FractalSSHCollection


def test_unit_FractalSSHCollection():
    credentials_A = dict(host="host", user="userA", key_path="/some/A")
    credentials_B = dict(host="host", user="userB", key_path="/some/B")

    # Create empty collection
    collection = FractalSSHCollection()
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

    # Pop a value from the collection
    popped_object_A = collection.pop(**credentials_A)
    assert popped_object_A is not None
    assert not collection.contains(**credentials_A)
    assert collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()

    # Pop a missing value from the collection
    popped_object_A = collection.pop(**credentials_A)
    assert popped_object_A is None
    assert not collection.contains(**credentials_A)
    assert collection.contains(**credentials_B)
    assert collection.size == 1
    assert not collection._lock.locked()


def test_run_command_through_collection(
    slurmlogin_ip,
    ssh_keys,
    fractal_ssh_collection: FractalSSHCollection,
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
    assert fractal_ssh_collection.contains(**valid_credentials)
    assert not fractal_ssh_collection.contains(**invalid_credentials)

    # Fetch and use the valid `FractalSSH` object
    valid_fractal_ssh = fractal_ssh_collection.get(**valid_credentials)
    stdout = valid_fractal_ssh.run_command(cmd="whoami")
    assert stdout.strip("\n") == "fractal"

    # Add the invalid `FractalSSH` object
    invalid_fractal_ssh = fractal_ssh_collection.get(**invalid_credentials)
    assert fractal_ssh_collection.contains(**invalid_credentials)

    # Try using the invalid `FractalSSH` object
    with pytest.raises(
        ValueError,
    ):
        invalid_fractal_ssh.run_command(cmd="ls")

    # Check that `close_all` works both for valid and invalid connections
    fractal_ssh_collection.close_all()
