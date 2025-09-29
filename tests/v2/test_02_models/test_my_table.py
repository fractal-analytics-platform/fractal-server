import pytest
from devtools import debug
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.v2 import MyTable


async def test_my_table_unique_contraint(db):
    db.add(MyTable(a=1, b=True))
    await db.commit()

    db.add(MyTable(a=1, b=False))
    await db.commit()

    db.add(MyTable(a=2, b=True))
    await db.commit()

    with pytest.raises(IntegrityError) as e:
        db.add(MyTable(a=1, b=True))
        await db.commit()
    debug(e.value)
