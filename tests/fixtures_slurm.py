import json
import logging
import shlex
import subprocess
from pathlib import Path
from typing import List
from typing import Sequence

import pytest
from devtools import debug


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
    https://github.com/sampsyo/clusterfutures/pull/19. The code of
    _jobs_finished is a copy of the function proposed via that PR.
    """

    import cfut
    import subprocess

    def _jobs_finished(job_ids: Sequence[str]):

        import logging

        logging.warning(f"[_jobs_finished] START {job_ids=}")
        if job_ids:
            assert type(list(job_ids)[0]) == str
        if not job_ids:
            logging.warning(
                "[_jobs_finished] NO JOBS TO CHECK, RETURN EMPTY SET"
            )
            return set()

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

        res = subprocess.run(
            [
                "squeue",
                "--noheader",
                # The next line is the one that gets changed via this fixture
                '--format="%i %T"',
                "--jobs",
                ",".join([j for j in job_ids]),
                "--states=all",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            check=True,
        )
        id_to_state = dict(
            [
                line.strip().partition(" ")[::2]
                for line in res.stdout.splitlines()
            ]
        )

        logging.basicConfig(format="%(asctime)s; %(levelname)s; %(message)s")
        logging.warning(f"[_jobs_finished] FROM SQUEUE: {id_to_state=}")

        finished_jobs = {
            j for j in job_ids if id_to_state.get(j, None) in STATES_FINISHED
        }
        logging.warning(f"[_jobs_finished] FROM SQUEUE {finished_jobs=}")
        # Finished jobs only stay in squeue for a few mins (configurable). If
        # a job ID isn't there, we'll assume it's finished.
        for job_id in job_ids:
            if (
                job_id not in finished_jobs
                and job_id not in id_to_state.keys()
            ):
                finished_jobs.add(job_id)
        logging.warning(
            f"[_jobs_finished] INCLUDING MISSING ONES {finished_jobs=}"
        )

        # ret = {
        #    j
        #    for j in map(str, job_ids)
        #    if id_to_state.get(j, "COMPLETED") in STATES_FINISHED
        # }
        # logging.warning(f"[_jobs_finished] {ret=}")
        return finished_jobs

    # Replace the jobs_finished function (from cfut.slurm) with our custom one
    monkeypatch.setattr(cfut.slurm, "jobs_finished", _jobs_finished)


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
