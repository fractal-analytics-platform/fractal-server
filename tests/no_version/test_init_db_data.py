import json
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select

from fractal_server.__main__ import init_db_data
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource


def test_init_db_data(db_sync, tmp_path: Path):
    init_db_data(resource="default", profile="default")
    assert db_sync.execute(select(func.count(Resource.id))).scalar() == 1
    assert db_sync.execute(select(func.count(Profile.id))).scalar() == 1

    with pytest.raises(SystemExit):
        init_db_data(resource="default", profile="default")


def test_init_db_data_failure(db_sync, tmp_path: Path):
    bad_json_file = tmp_path / "bad.json"
    with bad_json_file.open("w") as f:
        json.dump({"invalid": "resource"}, f)

    # with pytest.raises(SystemExit):
    #     init_db_data(resource=bad_json_file.as_posix(), profile="default")

    with pytest.raises(SystemExit):
        init_db_data(resource="default", profile=bad_json_file.as_posix())
