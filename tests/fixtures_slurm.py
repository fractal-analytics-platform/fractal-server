import logging
import os
import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa: E501
    _mkdir_as_user,
)
from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa: E501
    _run_command_as_user,
)

SLURM_USER = "test01"


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


@pytest.fixture
def patched_run_squeue(monkeypatch):
    """
    This fixture is a workaround to add quotes around the --format argument of
    squeue, see discussion in
    https://github.com/sampsyo/clusterfutures/pull/19.

    The code of run_squeue, below, is a copy of the function in
    fractal_server.app.runner.executors.slurm._check_jobs_status, with changes
    marked via # CHANGED comments.
    """

    import fractal_server.app.runner.executors.slurm_common._check_jobs_status  # noqa: E501
    from subprocess import run

    def patched_run_squeue(job_ids):
        logging.info(f"patched_run_squeue({job_ids})")
        res = run(  # nosec
            [
                "squeue",
                "--noheader",
                '--format="%i %T"',  # CHANGED
                "--jobs",
                ",".join([str(j) for j in job_ids]),
                "--states=all",
            ],
            capture_output=True,
            encoding="utf-8",
            check=False,
        )
        if res.returncode != 0:
            logging.warning(
                f"squeue command with {job_ids}"
                f" failed with:\n{res.stderr=}\n{res.stdout=}"
            )
        return res

    monkeypatch.setattr(
        fractal_server.app.runner.executors.slurm_common._check_jobs_status,
        "run_squeue",
        patched_run_squeue,
    )


@pytest.fixture
def monkey_slurm(
    monkeypatch,
    slurmlogin_container,
    patched_run_squeue,
):
    """
    Monkeypatch Popen to execute overridden command in container

    If sbatch is not present on the host machine, check if there is a
    containerised installation and redirect commands there. If no slurm
    container is present, xfail.
    """
    import subprocess

    OrigPopen = subprocess.Popen

    class PopenLog:
        calls: list[OrigPopen] = []

        @classmethod
        def add_call(cls, call):
            cls.calls.append(call)

        @classmethod
        def last_call(cls):
            return cls.calls[-1].args

    class _MockPopen(OrigPopen):
        def __init__(self, *args, **kwargs):
            cmd = args[0]
            assert isinstance(cmd, list)
            container_cmd = [" ".join(str(c) for c in cmd)]
            cmd = [
                "docker",
                "exec",
                "--user",
                "fractal",
                slurmlogin_container,
                "bash",
                "-c",
            ] + container_cmd
            super().__init__(cmd, *args[1:], **kwargs)
            logging.warning(shlex.join(self.args))
            PopenLog.add_call(self)

    monkeypatch.setattr(subprocess, "Popen", _MockPopen)
    return PopenLog


def run_squeue(squeue_format=None, header=True):
    cmd = ["squeue"]
    if not header:
        cmd.append("--noheader")
    if squeue_format:
        cmd.append(f'--format "{squeue_format}"')
    res = subprocess.run(cmd, capture_output=True, encoding="utf-8")
    if res.returncode != 0:
        debug(res.stderr)
    assert res.returncode == 0
    assert not res.stderr
    return res.stdout


def scancel_all_jobs_of_a_slurm_user(
    slurm_user: str, show_squeue: bool = True
):
    """
    Call scancel for all jobs of a given SLURM user
    """
    if show_squeue:
        debug(run_squeue())

    import logging

    logging.basicConfig(format="%(asctime)s; %(levelname)s; %(message)s")
    cmd = [
        "sudo",
        "--non-interactive",
        "-u",
        slurm_user,
        "scancel",
        "-u",
        slurm_user,
        "-v",
    ]
    logging.warning(
        f"Now running scancel_all_jobs_of_a_slurm_user with {cmd=}"
    )
    res = subprocess.run(
        cmd,
        capture_output=True,
        encoding="utf-8",
    )
    assert res.returncode == 0
    if res.stdout:
        debug(res.stdout)
    if res.stderr:
        debug(res.stderr)

    if show_squeue:
        debug(run_squeue())
    logging.warning(
        f"Now completed scancel_all_jobs_of_a_slurm_user with {cmd=}"
    )


@pytest.fixture
def slurm_working_folders(
    tmp777_path: Path,
):
    root_path = tmp777_path
    user = SLURM_USER

    # Define working folders
    working_dir_local = root_path / "server"
    working_dir_remote = root_path / "user"

    # Create server working folder
    umask = os.umask(0)
    working_dir_local.mkdir(parents=True, mode=0o755)
    os.umask(umask)

    # Create user working folder
    _mkdir_as_user(folder=str(working_dir_remote), user=user)

    yield (working_dir_local, working_dir_remote)

    logging.warning("[slurm_working_folders] Start cleanup")
    _run_command_as_user(
        cmd=f"chmod 777 {str(working_dir_remote)}", user=user, check=True
    )
    logging.warning("[slurm_working_folders] End cleanup")
