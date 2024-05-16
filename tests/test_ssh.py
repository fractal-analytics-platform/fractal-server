import shlex
import subprocess
import sys

from devtools import debug
from fabric.connection import Connection


def _run_locally(
    _cmd: str, stdin_content: str | None = None
) -> subprocess.CompletedProcess:
    print("CMD:\n", shlex.split(_cmd))
    res = subprocess.run(
        shlex.split(_cmd),
        capture_output=True,
        encoding="utf-8",
    )
    print(f"RETURNCODE:\n{res.returncode}")
    print(f"STDOUT:\n{res.stdout}")
    print(f"STDERR:\n{res.stderr}")
    print()
    if res.returncode != 0:
        sys.exit("ERROR")
    return res


def _run_ssh_command_in_tests(
    command: str,
    hostname: str,
    username: str,
    password: str,
):

    print("CMD:\n", shlex.split(command))
    with Connection(
        host=hostname,
        user=username,
        connect_kwargs={"password": password},
    ) as connection:

        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

    return res


def test_ssh(slurmlogin_container):

    # Get IP
    res = _run_locally(
        "docker inspect "
        "-f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        f"{slurmlogin_container}"
    )
    ip = res.stdout.strip()
    debug(ip)

    # Run hostname through ssh
    res = _run_ssh_command_in_tests(
        command="hostname",
        hostname=ip,
        username="fractal",
        password="fractal",
    )
    assert res.stdout.strip("\n") == "slurmlogin"
