import fractal_server
from fractal_server.ssh._fabric import FractalSSH


def test_versions(fractal_ssh: FractalSSH):
    """
    Check the Python and fractal-server versions available on the cluster.
    """

    command = "/usr/bin/python3.9 --version"
    print(f"COMMAND:\n{command}")
    stdout = fractal_ssh.run_command(cmd=command)
    print(f"STDOUT:\n{stdout}")

    python_command = "import fractal_server as fs; print(fs.__VERSION__);"
    command = f"/usr/bin/python3.9 -c '{python_command}'"

    print(f"COMMAND:\n{command}")
    stdout = fractal_ssh.run_command(cmd=command)
    print(f"STDOUT:\n{stdout}")
    assert stdout.strip() == str(fractal_server.__VERSION__)
