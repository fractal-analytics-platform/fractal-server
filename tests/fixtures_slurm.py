import json
import logging
import shlex
import subprocess
from copy import copy
from pathlib import Path
from typing import List

import pytest
from devtools import debug

import fractal_server
from fractal_server.app.runner._slurm import FractalSlurmExecutor


def is_responsive(container_name):
    try:
        import subprocess

        exec_cmd = ["docker", "ps", "-f", f"name={container_name}"]
        out = subprocess.run(exec_cmd, check=True, capture_output=True)
        if out.stdout.decode("utf-8") is not None:
            return True
    except ConnectionError:
        return False


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig, testdata_path: Path):

    import fractal_server
    import tarfile

    # This same path is hardocded in the Dockerfile of the SLURM node.
    CODE_ROOT = Path(fractal_server.__file__).parent.parent
    TAR_FILE = (
        testdata_path / "slurm_docker_images/node/fractal_server_local.tar.gz"
    )
    TAR_ROOT = CODE_ROOT.name
    with tarfile.open(TAR_FILE, "w:gz") as tar:
        tar.add(CODE_ROOT, arcname=TAR_ROOT, recursive=False)
        for name in ["pyproject.toml", "README.md", "fractal_server"]:
            f = CODE_ROOT / name
            tar.add(f, arcname=f.relative_to(CODE_ROOT.parent))

    return str(testdata_path / "slurm_docker_images/docker-compose.yml")


@pytest.fixture(scope="session")
def slurm_config(override_settings):
    # NOTE: override_settings also loads all environment variables from
    # get_patched_settings
    config = {
        "default": dict(partition="main", mem="1024"),
        "low": dict(partition="main", mem="128"),
        "cpu-low": dict(partition="main"),
    }

    with override_settings.FRACTAL_SLURM_CONFIG_FILE.open("w") as f:
        json.dump(config, f)
    return config


@pytest.fixture
def cfut_jobs_finished(monkeypatch):
    """
    This fixture is a workaround to add quotes around the --format argument of
    squeue, see discussion in
    https://github.com/sampsyo/clusterfutures/pull/19.

    The code of _jobs_finished, below, is a copy of the function from
    https://github.com/sampsyo/clusterfutures/blob/09792479c2b5d3fb27d840cbd76138fae6d802a1/cfut/slurm.py#L37-L54,
    with changes marked via # CHANGED comments.
    """

    import cfut
    from subprocess import run, PIPE

    def _jobs_finished(job_ids):
        """Check which ones of the given Slurm jobs already finished"""

        # CHANGED -- start
        import logging

        logging.basicConfig(format="%(asctime)s; %(levelname)s; %(message)s")
        logging.warning(f"[_jobs_finished] START {job_ids=}")
        # CHANGED -- end

        # CHANGED -- start
        STATES_FINISHED = {  # https://slurm.schedmd.com/squeue.html#lbAG
            "BOOT_FAIL",
            "CANCELLED",
            "COMPLETED",
            "DEADLINE",
            "FAILED",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "SPECIAL_EXIT",
            "TIMEOUT",
        }
        # CHANGED -- end

        # CHANGED -- start (only useful for debugging)
        if job_ids:
            assert type(list(job_ids)[0]) == str
        # CHANGED -- end

        # If there is no Slurm job to check, return right away
        if not job_ids:
            return set()

        res = run(
            [
                "squeue",
                "--noheader",
                '--format="%i %T"',  # CHANGED
                "--jobs",
                ",".join([str(j) for j in job_ids]),
                "--states=all",
            ],
            stdout=PIPE,
            stderr=PIPE,
            encoding="utf-8",
            check=True,
        )
        id_to_state = dict(
            [
                line.strip().partition(" ")[::2]
                for line in res.stdout.splitlines()
            ]
        )

        # CHANGED -- start
        logging.warning(f"[_jobs_finished] FROM SQUEUE: {id_to_state=}")
        # CHANGED -- end

        # Finished jobs only stay in squeue for a few mins (configurable). If
        # a job ID isn't there, we'll assume it's finished.
        # CHANGED -- start (print output before returning
        finished_jobs = {
            j
            for j in job_ids
            if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
        }
        logging.warning(
            f"[_jobs_finished] INCLUDING MISSING ONES {finished_jobs=}"
        )
        return finished_jobs
        # CHANGED -- end

    # Replace the jobs_finished function (from cfut.slurm) with our custom one
    monkeypatch.setattr(cfut.slurm, "jobs_finished", _jobs_finished)


@pytest.fixture
def monkey_slurm_user():
    return "test01"


@pytest.fixture
def monkey_slurm(monkeypatch, docker_compose_project_name, docker_services):
    """
    Monkeypatch Popen to execute overridden command in container

    If sbatch is not present on the host machine, check if there is a
    containerised installation and redirect commands there. If no slurm
    container is present, xfail.
    """
    import subprocess

    OrigPopen = subprocess.Popen

    slurm_container = docker_compose_project_name + "_slurm-docker-master_1"
    logging.warning(f"{docker_compose_project_name=}")
    logging.warning(f"{slurm_container=}")

    docker_services.wait_until_responsive(
        timeout=20.0,
        pause=0.5,
        check=lambda: is_responsive(slurm_container),
    )

    class PopenLog:
        calls: List[OrigPopen] = []

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
                slurm_container,
                "bash",
                "-c",
            ] + container_cmd
            super().__init__(cmd, *args[1:], **kwargs)
            debug(shlex.join(self.args))
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


def _create_folder_as_user(*, path: str, user: str, mode: int = None):
    res = subprocess.run(
        shlex.split(f"sudo -u {user} mkdir {path}"),
        encoding="utf-8",
        capture_output=True,
    )
    debug(res)
    assert res.returncode == 0
    if mode:
        res = subprocess.run(
            shlex.split(f"sudo -u {user} chmod {mode} {path}"),
            encoding="utf-8",
            capture_output=True,
        )
        debug(res)
        assert res.returncode == 0


@pytest.fixture
def patch_copy_method(monkeypatch):
    """
    This fixture is needed because of the two-users CI issue:
    * In real life, there is a single user running the server (typically named
      "fractal").
    * In the CI, there is the machine user running the server via pytest, and
      then there is the docker-container user named "fractal" (UID 2000, in
      docker) who runs the subprocess commands (thanks to the monkeypatch
      defined in the monkey_slurm fixture). The latter is also the user who has
      sudo privileges (within docker containers).

    Some files in the server working dir are owned by fractal (docker-container
    user), who created them via a command like

        sudo -u test01 cat /some/file > /some/path

    Therefore the machine user (who is running pytest and fractal-server)
    cannot overwrite them. This is only an issue within the CI, and we address
    it with the workaround of making all the relevant files broadly (777)
    accessible.
    """

    # Store a copy of the non-patched method, to avoid a recursion error in the
    # patched method
    original_method = copy(
        FractalSlurmExecutor._copy_files_from_user_to_server
    )

    def patched_method(self):
        logging.warning("Enter patched _copy_files_from_user_to_server")

        # Run original method, which iterates commands like `sudo -u test01 cat
        # /some/thing > /some/where` over many files, effectively copying them
        # from self.working_dir_user to self.working_dir
        original_method(self)

        # Make all destination files broadly accessible
        cmd = f"chmod 777 {str(self.working_dir)}/*"
        subprocess.run(shlex.split(cmd), capture_output=True, encoding="utf-8")

        # NOTE: res.returncode is typically != 0, because some files in that
        # folder belong to the machine user and not to the docker-container
        # fractal user. This is harmless, since the command will still act on
        # the fractal-owned files and leave the other ones untouched.

        logging.warning("Exit patched _copy_files_from_user_to_server")

    monkeypatch.setattr(
        fractal_server.app.runner._slurm.FractalSlurmExecutor,
        "_copy_files_from_user_to_server",
        patched_method,
    )
