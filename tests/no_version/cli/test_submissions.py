from sqlalchemy import select

from fractal_server.app.models import Resource
from fractal_server.cli._submissions import submissions_disable
from fractal_server.cli._submissions import submissions_enable


def test_submissions(
    db_sync,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
):
    current = (
        (db_sync.execute(select(Resource.prevent_new_submissions)))
        .scalars()
        .all()
    )
    assert current == [False, False]

    submissions_disable()

    current = (
        (db_sync.execute(select(Resource.prevent_new_submissions)))
        .scalars()
        .all()
    )
    assert current == [True, True]

    submissions_enable()

    current = (
        (db_sync.execute(select(Resource.prevent_new_submissions)))
        .scalars()
        .all()
    )
    assert current == [False, False]
