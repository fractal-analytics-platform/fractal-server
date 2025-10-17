from sqlalchemy import func
from sqlalchemy import select

from fractal_server.__main__ import init_db_data
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource


def test_init_db_data(db_sync):
    init_db_data(resource="default", profile="default")
    assert db_sync.execute(select(func.count(Resource.id))).scalar() == 1
    assert db_sync.execute(select(func.count(Profile.id))).scalar() == 1
