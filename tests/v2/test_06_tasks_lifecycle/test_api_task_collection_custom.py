import json
import sys

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskCollectCustom

PREFIX = "api/v2/task"


async def test_task_collection_custom(
    client,
    MockCurrentUser,
    fractal_tasks_mock_collection,
    tmp_path,
    local_resource_profile_db,
):
    package_name = "fractal_tasks_mock"
    python_bin = fractal_tasks_mock_collection["python_bin"].as_posix()
    manifest = fractal_tasks_mock_collection["manifest"]

    resource, profile = local_resource_profile_db
    # ---

    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True, profile_id=profile.id)
    ):
        payload_name = TaskCollectCustom(
            manifest=manifest,
            python_interpreter=python_bin,
            label="label",
            package_root=None,
            package_name=package_name,
        )

        # Fail because no package is installed in sys.executable

        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=(
                payload_name.model_dump()
                | dict(python_interpreter=sys.executable)
            ),
        )
        assert res.status_code == 422
        assert "Cannot determine 'package_root'" in res.json()["detail"]

        # Success with 'package_name'
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_name.model_dump()
        )
        assert res.status_code == 201

        # Success with package_name with hypens instead of underscore
        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=(
                payload_name.model_dump()
                | dict(
                    package_name=package_name.replace("_", "-"),
                    label="label2",
                )
            ),
        )
        assert res.status_code == 201

        # Success with package_root
        package_root = fractal_tasks_mock_collection["package_root"].as_posix()

        payload_root = TaskCollectCustom(
            manifest=manifest,
            python_interpreter=python_bin,
            label="label3",
            package_root=package_root,
            package_name=None,
        )
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.model_dump()
        )
        assert res.status_code == 201

        # Fail because python_interpreter does not exist
        payload_root.python_interpreter = "/foo/bar"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.model_dump()
        )
        assert res.status_code == 422
        assert "is not accessible" in res.json()["detail"]

        # Fail because python_interpreter is not valid
        payload_root.python_interpreter = tmp_path.as_posix()
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.model_dump()
        )
        assert res.status_code == 422
        assert "is not a file" in res.json()["detail"]

        # Fail because package_root does not exist
        payload_root.python_interpreter = sys.executable
        package_root_path = tmp_path / "foo"
        payload_root.package_root = package_root_path.as_posix()
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.model_dump()
        )
        assert res.status_code == 422
        assert "not accessible to the Fractal user" in res.json()["detail"]

        package_root_path.touch()
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.model_dump()
        )
        assert res.status_code == 422
        assert "is not a directory" in res.json()["detail"]


async def test_task_collection_custom_fail_with_ssh(
    client,
    MockCurrentUser,
    override_settings_factory,
    testdata_path,
    slurm_ssh_resource_profile_fake_db,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_SSH)
    manifest_file = (
        testdata_path.parent
        / "v2/fractal_tasks_mock"
        / "src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json"
    ).as_posix()

    with open(manifest_file) as f:
        manifest_dict = json.load(f)

    resource, profile = slurm_ssh_resource_profile_fake_db
    async with MockCurrentUser(
        user_kwargs=dict(is_verified=True, profile_id=profile.id)
    ):
        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=TaskCollectCustom(
                manifest=ManifestV2(**manifest_dict),
                python_interpreter="/may/not/exist",
                label="label",
                package_root=None,
                package_name="c",
            ).model_dump(),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot infer 'package_root' with 'slurm_ssh' backend."
        )
