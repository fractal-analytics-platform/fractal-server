import sys
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo
from fractal_server.app.schemas.v2 import ValidResourceLocal
from fractal_server.app.schemas.v2 import ValidResourceSlurmSSH
from fractal_server.app.schemas.v2 import ValidResourceSlurmSudo
from tests.fixtures_slurm import SLURM_USER

SLURM_CONFIG = {
    "default_slurm_config": {
        "partition": "main",
        "cpus_per_task": 1,
        "mem": "100M",
        "shebang_line": "#!/bin/bash",
        "use_mem_per_cpu": False,
    },
    "gpu_slurm_config": {},
    "batching_config": {
        "target_cpus_per_job": 1,
        "max_cpus_per_job": 1,
        "target_mem_per_job": 200,
        "max_mem_per_job": 500,
        "target_num_jobs": 2,
        "max_num_jobs": 4,
    },
}


def _add_resource_profile_to_db(
    *,
    res: Resource,
    prof: Profile,
    db_sync: Session,
) -> tuple[Resource, Profile]:
    db_sync.add(res)
    db_sync.commit()
    db_sync.refresh(res)
    db_sync.expunge(res)

    prof.resource_id = res.id
    db_sync.add(prof)
    db_sync.commit()
    db_sync.refresh(prof)
    db_sync.expunge(prof)

    return res, prof


@pytest.fixture(scope="function")
def local_resource_profile_objects(
    tmp777_path: Path,
    current_py_version: str,
) -> tuple[Resource, Profile]:
    """
    This fixture does not act on the db.
    """
    res = Resource(
        name="local resource 1",
        type=ResourceType.LOCAL,
        jobs_local_dir=(tmp777_path / "jobs").as_posix(),
        tasks_local_dir=(tmp777_path / "tasks").as_posix(),
        jobs_runner_config={"parallel_tasks_per_job": 1},
        tasks_python_config={
            "default_version": current_py_version,
            "versions": {
                current_py_version: sys.executable,
            },
        },
        tasks_pixi_config={},
        jobs_poll_interval=0,
    )
    prof = Profile(
        name="local_resource_profile_objects",
        resource_id=123456789,
        resource_type=ResourceType.LOCAL,
    )
    ValidResourceLocal(**res.model_dump())
    ValidProfileLocal(**prof.model_dump())
    return res, prof


@pytest.fixture(scope="function")
def slurm_sudo_resource_profile_objects(
    tmp777_path: Path,
    current_py_version: str,
) -> tuple[Resource, Profile]:
    """
    This fixture does not act on the db.
    """
    res = Resource(
        name="SLURM cluster A",
        type=ResourceType.SLURM_SUDO,
        jobs_local_dir=(tmp777_path / "local-jobs").as_posix(),
        tasks_local_dir=(tmp777_path / "local-tasks").as_posix(),
        jobs_slurm_python_worker=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        ),
        jobs_runner_config=SLURM_CONFIG,
        tasks_python_config={
            "default_version": current_py_version,
            "versions": {
                v: f"/.venv{v}/bin/python{v}"
                for v in (
                    "3.10",
                    "3.11",
                    "3.12",
                    "3.13",
                    "3.14",
                )
            },
        },
        tasks_pixi_config={},
        jobs_poll_interval=0,
    )
    prof = Profile(
        name="slurm_sudo_resource_profile_objects",
        resource_id=123456789,
        username=SLURM_USER,
        resource_type=ResourceType.SLURM_SUDO,
    )
    ValidResourceSlurmSudo(**res.model_dump())
    ValidProfileSlurmSudo(**prof.model_dump())

    return res, prof


@pytest.fixture(scope="function")
def slurm_ssh_resource_profile_objects(
    tmp777_path: Path,
    current_py_version: str,
    ssh_keys: dict[str, str],
    slurmlogin_ip: str,
    ssh_username: str,
) -> tuple[Resource, Profile]:
    """
    This fixture does not act on the db.
    """
    res = Resource(
        name="SLURM cluster A",
        type=ResourceType.SLURM_SSH,
        host=slurmlogin_ip,
        jobs_local_dir=(tmp777_path / "local-jobs").as_posix(),
        tasks_local_dir=(tmp777_path / "local-tasks").as_posix(),
        jobs_slurm_python_worker=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        ),
        jobs_runner_config=SLURM_CONFIG,
        tasks_python_config={
            "default_version": current_py_version,
            "versions": {
                v: f"/.venv{v}/bin/python{v}"
                for v in (
                    "3.10",
                    "3.11",
                    "3.12",
                    "3.13",
                    "3.14",
                )
            },
        },
        tasks_pixi_config={},
        jobs_poll_interval=0,
    )
    prof = Profile(
        resource_id=123456789,
        resource_type=ResourceType.SLURM_SSH,
        name="slurm_ssh_resource_profile_objects",
        username=ssh_username,
        ssh_key_path=ssh_keys["private"],
        jobs_remote_dir=(tmp777_path / "remote-jobs").as_posix(),
        tasks_remote_dir=(tmp777_path / "remote-tasks").as_posix(),
    )
    ValidResourceSlurmSSH(**res.model_dump())
    ValidProfileSlurmSSH(**prof.model_dump())

    return res, prof


@pytest.fixture(scope="function")
def slurm_ssh_resource_profile_fake_objects(
    tmp777_path: Path,
    current_py_version: str,
    ssh_username: str,
) -> tuple[Resource, Profile]:
    """
    This fixture does not require an active SSH service on a container.
    """
    res = Resource(
        name="SLURM cluster A",
        type=ResourceType.SLURM_SSH,
        host="localhost",
        jobs_local_dir=(tmp777_path / "local-jobs").as_posix(),
        tasks_local_dir=(tmp777_path / "local-tasks").as_posix(),
        jobs_slurm_python_worker=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        ),
        jobs_runner_config=SLURM_CONFIG,
        tasks_python_config={
            "default_version": current_py_version,
            "versions": {
                v: f"/.venv{v}/bin/python{v}"
                for v in (
                    "3.10",
                    "3.11",
                    "3.12",
                    "3.13",
                    "3.14",
                )
            },
        },
        tasks_pixi_config={},
        jobs_poll_interval=0,
    )
    prof = Profile(
        resource_id=123456789,
        resource_type=ResourceType.SLURM_SSH,
        name="slurm_ssh_resource_profile_fake_objects",
        username=ssh_username,
        ssh_key_path="/fake/key",
        jobs_remote_dir="/fake/jobs",
        tasks_remote_dir="/fake/tasks",
    )
    ValidResourceSlurmSSH(**res.model_dump())
    ValidProfileSlurmSSH(**prof.model_dump())

    return res, prof


@pytest.fixture(scope="function")
def local_resource_profile_db(
    db_sync: Session,
    local_resource_profile_objects: tuple[Resource, Profile],
) -> tuple[Resource, Profile]:
    res, prof = local_resource_profile_objects
    return _add_resource_profile_to_db(
        res=res,
        prof=prof,
        db_sync=db_sync,
    )


@pytest.fixture(scope="function")
def slurm_sudo_resource_profile_db(
    db_sync: Session,
    slurm_sudo_resource_profile_objects: tuple[Resource, Profile],
) -> tuple[Resource, Profile]:
    res, prof = slurm_sudo_resource_profile_objects[:]
    return _add_resource_profile_to_db(
        res=res,
        prof=prof,
        db_sync=db_sync,
    )


@pytest.fixture(scope="function")
def slurm_ssh_resource_profile_db(
    db_sync: Session,
    slurm_ssh_resource_profile_objects: tuple[Resource, Profile],
) -> tuple[Resource, Profile]:
    res, prof = slurm_ssh_resource_profile_objects[:]
    return _add_resource_profile_to_db(
        res=res,
        prof=prof,
        db_sync=db_sync,
    )


@pytest.fixture(scope="function")
def slurm_ssh_resource_profile_fake_db(
    db_sync: Session,
    slurm_ssh_resource_profile_fake_objects: tuple[Resource, Profile],
) -> tuple[Resource, Profile]:
    res, prof = slurm_ssh_resource_profile_fake_objects[:]
    return _add_resource_profile_to_db(
        res=res,
        prof=prof,
        db_sync=db_sync,
    )
