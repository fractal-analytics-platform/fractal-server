import io

from fabric.connection import Connection


def test_unit_fabric_connection(slurmlogin_ip, monkeypatch):
    """
    Test both the pytest-docker setup and the use of a `fabric` connection, by
    running the `hostname` over SSH.
    """
    print(f"{slurmlogin_ip=}")

    print("subprocess SSH - start")
    import subprocess
    import shlex

    res = subprocess.run(
        shlex.split(f"ssh fractal@{slurmlogin_ip} -vvv"),
        capture_output=True,
        encoding="utf-8",
    )
    print("RETCODE:")
    print(res.returncode)
    print("STDOUT:")
    print(res.stdout)
    print("STDERR:")
    print(res.stderr)
    print("subprocess SSH - end")

    command = "hostname"
    print(f"Now run {command=} at {slurmlogin_ip=}")

    # https://github.com/fabric/fabric/issues/1979
    # https://github.com/fabric/fabric/issues/2005#issuecomment-525664468
    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        connect_kwargs={"password": "fractal"},
    ) as connection:

        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")
        assert res.stdout.strip("\n") == "slurmhead"
