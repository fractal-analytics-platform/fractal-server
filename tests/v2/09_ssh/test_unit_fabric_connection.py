from fabric.connection import Connection


def test_unit_fabric_connection(slurmlogin_ip):
    """
    Test both the pytest-docker setup and the use of a `fabric` connection, by
    running the `hostname` over SSH.
    """
    print(f"{slurmlogin_ip=}")

    command = "hostname"
    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        connect_kwargs={"password": "fractal"},
    ) as connection:

        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")
        assert res.stdout.strip("\n") == "slurmhead"
