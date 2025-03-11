import pytest
from fabric.connection import Connection

import fractal_server
from fractal_server.ssh._fabric import FractalSSH


def test_check_connection_failure():

    with Connection(
        host="localhost",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        this_fractal_ssh = FractalSSH(connection=connection)
        with pytest.raises(RuntimeError):
            this_fractal_ssh.check_connection()
        this_fractal_ssh.close()


@pytest.mark.container
@pytest.mark.ssh
def test_versions(fractal_ssh: FractalSSH, current_py_version: str):
    """
    Check the Python and fractal-server versions available on the cluster.
    """

    python_bin = f"/.venv{current_py_version}/bin/python{current_py_version}"
    command = f"{python_bin} --version"
    print(f"COMMAND:\n{command}")
    stdout = fractal_ssh.run_command(cmd=command)
    print(f"STDOUT:\n{stdout}")

    python_command = "import fractal_server as fs; print(fs.__VERSION__);"
    command = f"{python_bin} -c '{python_command}'"

    print(f"COMMAND:\n{command}")

    with pytest.raises(ValueError):
        fractal_ssh.run_command(cmd=command)
    stdout = fractal_ssh.run_command(cmd=command, allow_char=";'()")

    print(f"STDOUT:\n{stdout}")
    assert stdout.strip() == str(fractal_server.__VERSION__)
