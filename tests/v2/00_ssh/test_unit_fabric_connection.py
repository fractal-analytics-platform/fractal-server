import io
import shlex
import subprocess

from fabric.connection import Connection


def _run_locally(cmd):
    print("COMMAND:")
    print(cmd)
    res = subprocess.run(
        shlex.split(cmd),
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


def test_unit_fabric_connection(slurmlogin_ip, monkeypatch):
    """
    Test both the pytest-docker setup and the use of a `fabric` connection, by
    running the `hostname` over SSH.
    """
    print(f"{slurmlogin_ip=}")

    cmd = "docker exec --user root slurm_docker_images-slurmhead-1 ip a"
    _run_locally(cmd)
    cmd = "docker exec --user root slurm_docker_images-slurmhead-1 service ssh status"
    _run_locally(cmd)
    cmd = (
        "docker exec --user root slurm_docker_images-slurmhead-1 lsof -i -n -P"
    )
    _run_locally(cmd)
    cmd = "docker exec --user root slurm_docker_images-slurmhead-1 grep Port /etc/ssh/sshd_config"
    _run_locally(cmd)
    cmd = f"ssh fractal@{slurmlogin_ip} -vvv"
    _run_locally(cmd)

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
