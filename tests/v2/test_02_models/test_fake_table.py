import pytest
from devtools import debug

from fractal_server.app.models.v2 import FakeTable


async def test_fake_table_unique_contraint(db):
    db.add(FakeTable(a=1, b=True))
    await db.commit()

    db.add(FakeTable(a=1, b=False))
    await db.commit()

    db.add(FakeTable(a=2, b=True))
    await db.commit()

    with pytest.raises(Exception) as e:
        db.add(FakeTable(a=1, b=True))
        await db.commit()
    debug(e.value)
