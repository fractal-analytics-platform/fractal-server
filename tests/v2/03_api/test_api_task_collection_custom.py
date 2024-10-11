import json
import sys

from devtools import debug  # noqa

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCollectCustomV2


PREFIX = "api/v2/task"


async def test_task_collection_custom(
    client,
    MockCurrentUser,
    fractal_tasks_mock_collection,
):
    package_name = "fractal_tasks_mock"
    python_bin = fractal_tasks_mock_collection["python_bin"].as_posix()
    manifest = fractal_tasks_mock_collection["manifest"]

    # ---

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):

        payload_name = TaskCollectCustomV2(
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
                payload_name.dict() | dict(python_interpreter=sys.executable)
            ),
        )
        assert res.status_code == 422
        assert "Cannot determine 'package_root'" in res.json()["detail"]

        # Success with 'package_name'
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_name.dict()
        )
        assert res.status_code == 201

        # Success with package_name with hypens instead of underscore
        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=(
                payload_name.dict()
                | dict(
                    package_name=package_name.replace("_", "-"),
                    label="label2",
                )
            ),
        )
        assert res.status_code == 201

        # Success with package_root
        package_root = fractal_tasks_mock_collection["package_root"].as_posix()

        payload_root = TaskCollectCustomV2(
            manifest=manifest,
            python_interpreter=python_bin,
            label="label3",
            package_root=package_root,
            package_name=None,
        )
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 201

        # Fail because python_interpreter does not exist
        payload_root.python_interpreter = "/foo/bar"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "doesn't exist or is not a file" in res.json()["detail"]

        # Fail because package_root does not exist
        payload_root.python_interpreter = sys.executable
        payload_root.package_root = "/foo/bar"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "doesn't exist or is not a directory" in res.json()["detail"]


async def test_task_collection_custom_fail_with_ssh(
    client,
    MockCurrentUser,
    override_settings_factory,
    testdata_path,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm_ssh")
    manifest_file = (
        testdata_path.parent
        / "v2/fractal_tasks_mock"
        / "src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json"
    ).as_posix()

    with open(manifest_file, "r") as f:
        manifest_dict = json.load(f)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=TaskCollectCustomV2(
                manifest=ManifestV2(**manifest_dict),
                python_interpreter="/may/not/exist",
                label="label",
                package_root=None,
                package_name="c",
            ).dict(),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot infer 'package_root' with 'slurm_ssh' backend."
        )
