import logging
import shlex
import subprocess

from devtools import debug
from fabric.connection import Connection

from .fixtures_slurm import is_responsive


def _run_ssh_command_in_tests(
    command: str,
    hostname: str,
    username: str,
    password: str,
):

    # hostname = "cluster.s3it.uzh.ch"
    # username = "srv-mls-prbvc"
    # key_path = "/home/ubuntu/.ssh/service_user_sciencecluster.key"
    connection = Connection(
        host=hostname,
        user=username,
        # connect_kwargs={"key_filename": key_path},
        connect_kwargs={"password": password},
    )

    result = connection.run(command, hide=True)
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)
    connection.close()

    return result


def test_ssh(docker_services, docker_compose_project_name, docker_ip):

    return

    slurm_container = docker_compose_project_name + "-slurm-docker-master-1"
    logging.warning(f"{docker_compose_project_name=}")
    logging.warning(f"{slurm_container=}")

    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )

    debug(docker_services)
    debug(docker_ip)

    def _run(_cmd: str, stdin_content: str | None = None):
        print("CMD:\n", shlex.split(_cmd))
        proc = subprocess.Popen(
            shlex.split(_cmd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        if stdin_content is not None:
            print(f"STDIN:\n{stdin_content}")
            # proc.stdin.write(stdin_content)  # FIXME: not working
            stdout, stderr = proc.communicate(
                input=stdin_content
            )  # FIXME not working
        else:
            stdout, stderr = proc.communicate()
        # proc.wait()
        print(f"RETURNCODE:\n{proc.returncode}")
        print(f"STDOUT:\n{stdout}")
        print(f"STDERR:\n{stderr}")
        print()
        return stdout, stderr

    _run(f"docker exec --user root {slurm_container} /usr/sbin/sshd")
    stdout, stderr = _run(
        "docker inspect "
        "-f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        f"{slurm_container}"
    )
    ip = stdout.strip()
    debug(ip)
    _run_ssh_command_in_tests(
        command="whoami",
        hostname=ip,
        username="fractal",
        password="fractal",
    )
