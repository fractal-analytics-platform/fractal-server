from devtools import debug
from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import Example
from fractal_server.urls import url_is_relative_to

with next(get_sync_db()) as db:
    db.add_all(
        [
            Example(example_path="/a"),
            Example(example_path="/a/b/c"),
            Example(example_path="/ab/c"),
            Example(example_path="s3://x"),
            Example(example_path="s3://x/y/z"),
            Example(example_path="s3://xy/z"),
        ]
    )
    db.commit()

    query = select(Example).where(Example.example_path.startswith("/a"))
    resources = db.execute(query).scalars().all()
    debug(resources)

    try:
        _ = select(Example).where(
            url_is_relative_to(url=Example.example_path, base="/a")
        )
        raise RuntimeError
    except AttributeError as e:
        debug(e)
