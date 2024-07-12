import io

import pytest
from fabric.connection import Connection

from fractal_server.ssh._fabric import FractalSSH


def test_unit_fabric_connection(
    slurmlogin_ip, ssh_alive, slurmlogin_container, monkeypatch
):
    """
    Test both the pytest-docker setup and the use of a `fabric` connection, by
    running the `hostname` over SSH.
    """
    print(f"{slurmlogin_ip=}")

    command = "hostname"
    print(f"Now run {command=} at {slurmlogin_ip=}")

    # https://github.com/fabric/fabric/issues/1979
    # https://github.com/fabric/fabric/issues/2005#issuecomment-525664468
    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        forward_agent=False,
        connect_kwargs={"password": "fractal"},
    ) as connection:

        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")
        assert res.stdout.strip("\n") == "slurmhead"

        # Test also FractalSSH
        fractal_conn = FractalSSH(connection=connection)
        assert fractal_conn.is_connected
        fractal_conn.check_connection()
        res = fractal_conn.run(command, hide=True)
        assert res.stdout.strip("\n") == "slurmhead"

    with Connection(
        host=slurmlogin_ip,
        user="x",
        connect_kwargs={"password": "x"},
    ) as connection:
        fractal_conn = FractalSSH(connection=connection)
        # raise error if there is not a connection available
        with pytest.raises(RuntimeError):
            fractal_conn.check_connection()
