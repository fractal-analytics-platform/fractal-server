import logging
from pathlib import Path

import pytest
from fabric.connection import Connection
from paramiko.ssh_exception import NoValidConnectionsError

from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import _acquire_lock_with_timeout
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHTimeoutError


logger = set_logger(__file__)


def test_acquire_lock():
    """
    Test that the lock cannot be acquired twice.
    """
    with Connection("localhost") as connection:
        fake_fractal_ssh = FractalSSH(connection=connection)
        fake_fractal_ssh._lock.acquire(timeout=0)
        with pytest.raises(FractalSSHTimeoutError) as e:
            with _acquire_lock_with_timeout(
                lock=fake_fractal_ssh._lock,
                timeout=0.1,
                label="fail",
            ):
                pass
        print(e)


def test_fail_and_raise(tmp_path: Path, caplog):
    """
    Test Exception when `e.errors` is not an iterable.
    """

    class MyError(Exception):
        errors = 0

    class MockFractalSSH(FractalSSH):
        @property
        def _sftp_unsafe(self):
            raise MyError()

    LOGGER_NAME = "invalid_ssh"
    with Connection(
        host="localhost",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        mocked_fractal_ssh = MockFractalSSH(
            connection=connection, logger_name=LOGGER_NAME
        )

        logger = logging.getLogger(LOGGER_NAME)
        logger.propagate = True

        with pytest.raises(MyError):
            mocked_fractal_ssh.send_file(
                local="/invalid/local",
                remote="/invalid/remote",
            )
        log_text = caplog.text
        assert "Unexpected" in log_text
        assert "'int' object is not iterable" in log_text


def test_remove_folder_input_validation():
    """
    Test input validation of `remove_folder` method.
    """
    with Connection(host="localhost") as connection:
        fake_fractal_ssh = FractalSSH(connection=connection)

        # Folders which are just invalid
        invalid_folders = [
            None,
            "   /somewhere",
            "/ somewhere",
            "somewhere",
            "$(pwd)",
            "`pwd`",
        ]
        for folder in invalid_folders:
            with pytest.raises(ValueError) as e:
                fake_fractal_ssh.remove_folder(folder=folder, safe_root="/")
            print(e.value)

        # Folders which are just invalid
        invalid_folders = [
            None,
            "   /somewhere",
            "/ somewhere",
            "somewhere",
            "$(pwd)",
            "`pwd`",
        ]
        for safe_root in invalid_folders:
            with pytest.raises(ValueError) as e:
                fake_fractal_ssh.remove_folder(
                    folder="/tmp/something",
                    safe_root=safe_root,
                )
            print(e.value)

        # Folders which are not relative to the accepted root
        with pytest.raises(ValueError) as e:
            fake_fractal_ssh.remove_folder(folder="/", safe_root="/tmp")
        print(e.value)

        with pytest.raises(ValueError) as e:
            fake_fractal_ssh.remove_folder(
                folder="/actual_root/../something",
                safe_root="/actual_root",
            )
        print(e.value)


def test_novalidconnectionserror_in_sftp_methods(caplog):
    """
    Test `NoValidConnectionError`s in SFTP-based methods.
    """

    with Connection(
        host="localhost",
        port="8022",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        LOGGER_NAME = "invalid_ssh"
        fractal_ssh = FractalSSH(
            connection=connection, logger_name=LOGGER_NAME
        )
        logger = logging.getLogger(LOGGER_NAME)
        logger.propagate = True

        # Fail in several methods, because connection is closed

        caplog.clear
        with pytest.raises(NoValidConnectionsError):
            fractal_ssh.write_remote_file(path="/invalid", content="..")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(NoValidConnectionsError):
            fractal_ssh.send_file(local="/invalid", remote="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(NoValidConnectionsError):
            fractal_ssh.fetch_file(local="/invalid", remote="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(NoValidConnectionsError):
            fractal_ssh.remote_exists(path="/invalid")
        assert "NoValidConnectionsError" in caplog.text

        caplog.clear
        with pytest.raises(NoValidConnectionsError):
            fractal_ssh.read_remote_json_file(
                filepath="/invalid",
            )
        assert "NoValidConnectionsError" in caplog.text

        fractal_ssh.close()
