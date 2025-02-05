import pytest
from fabric.connection import Connection

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
