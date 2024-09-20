from fractal_server.ssh._fabric import FractalSSHCollection


def test_unit_FractalSSHCollection():

    credentials_A = dict(host="host", user="userA", key_path="/some/A")
    credentials_B = dict(host="host", user="userB", key_path="/some/B")

    # Create empty collection
    collection = FractalSSHCollection()
    assert collection._data == {}
    assert not collection._lock.locked()

    # Add a value to the collection for the first time
    fractal_ssh_A_first = collection.get(**credentials_A)
    first_id_object_A = id(fractal_ssh_A_first)
    assert len(collection._data) == 1
    assert not collection._lock.locked()

    # Re-add the same value to the collection
    fractal_ssh_A_second = collection.get(**credentials_A)
    second_id_object_A = id(fractal_ssh_A_second)
    assert len(collection._data) == 1
    assert not collection._lock.locked()

    # Calling `get` twice returns the same Python object
    assert first_id_object_A == second_id_object_A

    # Add a second value to the collection
    fractal_ssh_B = collection.get(**credentials_B)
    assert len(collection._data) == 2
    assert not collection._lock.locked()
    assert id(fractal_ssh_B) != first_id_object_A

    # Pop a value from the collection
    popped_object_A = collection.pop(**credentials_A)
    assert popped_object_A is not None
    assert len(collection._data) == 1
    assert not collection._lock.locked()

    # Pop a missing value from the collection
    popped_object_A = collection.pop(**credentials_A)
    assert popped_object_A is None
    assert len(collection._data) == 1
    assert not collection._lock.locked()


# def test_run_command(fractal_ssh_collection: FractalSSHCollection):
