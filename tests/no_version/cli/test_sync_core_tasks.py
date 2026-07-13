import json
import shlex
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from fractal_server.cli._sync_core_tasks import _count_core_tasks
from fractal_server.cli._sync_core_tasks import _get_final_set
from fractal_server.cli._sync_core_tasks import sync_core_tasks


def test_get_final_set(tmp_path: Path):
    path_invalid = tmp_path / "invalid.json"
    path_base = tmp_path / "base.json"
    path_add = tmp_path / "add.json"
    path_remove = tmp_path / "remove.json"

    path_base.write_text(
        json.dumps(
            [
                ["pkg1", "1.0.0", "A"],
                ["pkg1", "1.0.0", "A"],  # Repetition
                ["pkg1", "1.0.0", "A"],  # Repetition
                ["pkg1", "1.0.0", "B"],
                ["pkg1", "1.0.0", "C"],
                ["pkg2", "1.2.3", "A"],
                ["pkg2", "1.2.3", "D"],
            ]
        )
    )
    path_add.write_text(
        json.dumps(
            [
                ["pkg2", "1.2.3", "D"],
                ["pkg3", "1.2.3", "D"],
                ["pkg3", "1.2.3", "E"],
            ]
        )
    )
    path_remove.write_text(
        json.dumps(
            [
                ["pkg1", "1.0.0", "A"],
                ["pkg1", "1.0.0", "B"],
            ]
        )
    )

    assert _get_final_set() == set()

    with pytest.raises(ValidationError):
        path_invalid.write_text(json.dumps([["a", "b"]]))
        _get_final_set(base=path_invalid)

    final_list = _get_final_set(
        base=path_base,
        additions=path_add,
        removals=path_remove,
    )
    print(final_list)
    assert final_list == {
        ("pkg2", "1.2.3", "A"),
        ("pkg3", "1.2.3", "D"),
        ("pkg2", "1.2.3", "D"),
        ("pkg1", "1.0.0", "C"),
        ("pkg3", "1.2.3", "E"),
    }


async def test_sync_core_tasks(
    db_sync,
    tmp_path,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
    user_group_factory,
    MockCurrentUser,
    task_factory,
):
    resource1, profile1 = local_resource_profile_db
    resource2, profile2 = slurm_ssh_resource_profile_fake_db
    async with MockCurrentUser(profile_id=profile1.id) as user1:
        user1_id = user1.id
    async with MockCurrentUser(profile_id=profile2.id) as user2:
        user2_id = user2.id
    async with MockCurrentUser(profile_id=profile2.id) as user3:
        user3_id = user3.id
    user_group1 = await user_group_factory(
        group_name="my group 1", user_id=user1_id
    )
    user_group2 = await user_group_factory(
        group_name="my group 2", user_id=user2_id
    )
    user_group3 = await user_group_factory(
        group_name="my group 3", user_id=user3_id
    )
    user_group1_id: int = user_group1.id
    user_group2_id: int = user_group2.id
    user_group3_id: int = user_group3.id

    for user_id, user_group_id, resource_id in zip(
        [user1_id, user2_id, user3_id],
        [user_group1_id, user_group2_id, user_group3_id],
        [resource1.id, resource2.id, resource2.id],
    ):
        for pkg_name in ["pkg-a", "pkg-b"]:
            for version in ["1.0.0", "2.0.0"]:
                await task_factory(
                    user_id,
                    task_group_kwargs=dict(
                        pkg_name=pkg_name,
                        version=version,
                        user_group_id=user_group_id,
                        resource_id=resource_id,
                    ),
                    name="my task",
                    is_core=True,
                )
                print(f"{user_id=}, {user_group_id=}, {resource_id=}")

    path_resources_usergroups = tmp_path / "resources-usergroups.json"
    path_resources_usergroups.write_text(
        json.dumps(
            [{"resource_id": resource1.id, "user_group_id": user_group1_id}]
        )
    )

    assert _count_core_tasks(db_sync) > 0

    # (1) no core tasks selection
    sync_core_tasks(resources_and_groups=path_resources_usergroups)
    assert _count_core_tasks(db_sync) == 0

    # (2) Core tasks on a single resource
    path_base = tmp_path / "base.json"
    path_base.write_text(
        json.dumps(
            [
                ["pkg-a", "1.0.0", "my task"],
                ["pkg-a", "1.0.0", "my task"],  # duplicate
                ["pkg-missing", "1.0.0", "my task"],  # missing task group
                ["pgk-a", "1.0.0", "my missing task"],  # missing task
            ]
        )
    )
    sync_core_tasks(
        resources_and_groups=path_resources_usergroups, base=path_base
    )
    assert _count_core_tasks(db_sync) == 1

    # (3) Core tasks on three resources (but missing on the third one)
    path_resources_usergroups.write_text(
        json.dumps(
            [
                {"resource_id": resource1.id, "user_group_id": user_group1_id},
                {"resource_id": resource2.id, "user_group_id": user_group1_id},
                {
                    "resource_id": resource2.id,
                    "user_group_id": user_group3_id,
                },  # missing
            ]
        )
    )
    sync_core_tasks(
        resources_and_groups=path_resources_usergroups, base=path_base
    )
    assert _count_core_tasks(db_sync) == 2

    # (4) Failure and roll-back
    bad_path_resources_usergroups = tmp_path / "bad-resources-usergroups.json"
    bad_path_resources_usergroups.write_text(
        json.dumps(
            [
                {"resource_id": resource1.id, "user_group_id": user_group1_id},
                {"resource_id": resource2.id},
            ]
        )
    )
    path_base.write_text(
        json.dumps(
            [
                ["pkg-a", "1.0.0", "my task"],
                ["pkg-b", "1.0.0", "my task"],
                ["pkg-b", "2.0.0", "my task"],
            ]
        )
    )
    with pytest.raises(SystemExit):
        sync_core_tasks(
            resources_and_groups=bad_path_resources_usergroups, base=path_base
        )
    assert _count_core_tasks(db_sync) == 2

    # (5) Successful version of the failed run
    sync_core_tasks(
        resources_and_groups=path_resources_usergroups, base=path_base
    )
    assert _count_core_tasks(db_sync) == 6


def test_fractalctl_sync_core_tasks_cli():
    cmd = "fractalctl sync-core-tasks"

    res = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        encoding="utf-8",
    )
    assert res.returncode != 0
    assert "the following arguments are required" in res.stderr


def test_fractalctl_sync_core_tasks_run(monkeypatch):
    import argparse as ap

    def parse_args_mock() -> ap.Namespace:
        return ap.Namespace(
            cmd="sync-core-tasks",
            resources_and_groups="fake",
            base=None,
            additions=None,
            removals=None,
        )

    import fractal_server.cli.__main__
    from fractal_server.cli.__main__ import run

    monkeypatch.setattr(
        fractal_server.cli.__main__,
        "parse_args",
        parse_args_mock,
    )

    with pytest.raises(AttributeError):
        run()
