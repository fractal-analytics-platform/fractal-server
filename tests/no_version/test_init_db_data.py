import json
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select

from fractal_server.__main__ import init_db_data
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource


def test_init_db_data_default(db_sync):
    init_db_data(resource="default", profile="default")
    assert db_sync.execute(select(func.count(Resource.id))).scalar() == 1
    assert db_sync.execute(select(func.count(Profile.id))).scalar() == 1

    with pytest.raises(SystemExit):
        init_db_data(resource="default", profile="default")


def test_init_db_data_resource_and_profile(
    tmp_path, local_resource_profile_objects
):
    resource, profile = local_resource_profile_objects
    resource_path = tmp_path / "resource.json"
    profile_path = tmp_path / "profile.json"

    with resource_path.open("w") as f:
        json.dump(resource.model_dump(exclude={"id", "timestamp_created"}), f)
    with profile_path.open("w") as f:
        json.dump(profile.model_dump(exclude={"id"}), f)

    with pytest.raises(SystemExit):
        init_db_data(resource=resource_path.as_posix())

    with pytest.raises(SystemExit):
        init_db_data(profile=profile_path.as_posix())

    init_db_data(
        resource=resource_path.as_posix(), profile=profile_path.as_posix()
    )


def test_init_db_data_user_and_password(db_sync):
    email = "admin@example.org"
    password = "1234"

    with pytest.raises(SystemExit):
        init_db_data(admin_password=password)

    with pytest.raises(SystemExit):
        init_db_data(admin_email=email)

    init_db_data(admin_email=email, admin_password=password)


def test_init_db_data_from_file(
    db_sync,
    local_resource_profile_objects,
    tmp_path,
):
    resource, profile = local_resource_profile_objects
    resource.tasks_pixi_config = {
        "default_version": "0.41.0",
        "versions": {"0.41.0": "/xxx/pixi/0.41.0/"},
    }
    res_json_file = tmp_path / "res.json"
    with res_json_file.open("w") as f:
        json.dump(
            resource.model_dump(exclude={"id", "timestamp_created"}),
            f,
        )
    prof_json_file = tmp_path / "prof.json"
    with prof_json_file.open("w") as f:
        json.dump(
            profile.model_dump(exclude={"id", "resource_id"}),
            f,
        )
    init_db_data(
        resource=res_json_file.as_posix(),
        profile=prof_json_file.as_posix(),
    )

    db_resource = db_sync.execute(select(Resource)).scalars().first()
    assert "TOKIO_WORKER_THREADS" in db_resource.tasks_pixi_config


def test_init_db_data_failure(db_create_tables, tmp_path: Path):
    bad_json_file = tmp_path / "bad.json"
    with bad_json_file.open("w") as f:
        json.dump({"invalid": "resource"}, f)

    with pytest.raises(SystemExit):
        init_db_data(resource=bad_json_file.as_posix(), profile="default")

    with pytest.raises(SystemExit):
        init_db_data(resource="default", profile=bad_json_file.as_posix())
