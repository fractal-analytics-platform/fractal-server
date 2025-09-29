import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.v2 import MyTable


async def test_my_table_unique_contraint(db):
    db.add(MyTable(a=1, b=True))
    await db.commit()

    db.add(MyTable(a=1, b=False))
    await db.commit()
    db.add(MyTable(a=1, b=False))
    await db.commit()

    db.add(MyTable(a=2, b=True))
    await db.commit()

    with pytest.raises(IntegrityError):
        db.add(MyTable(a=1, b=True))
        await db.commit()
    await db.rollback()

    db.add(MyTable(a=1, b=False))
    await db.commit()
    db.add(MyTable(a=1, b=False))
    await db.commit()

    with pytest.raises(IntegrityError):
        db.add(MyTable(a=1, b=True))
        await db.commit()
    await db.rollback()
