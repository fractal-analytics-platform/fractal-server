from datetime import datetime

from devtools import debug
from schemas import _StateBase
from schemas import StateRead


def test_state():
    s = _StateBase(data={"some": "thing"}, timestamp=datetime.now())
    debug(s)
    debug(s.sanitised_dict())
    assert isinstance(s.sanitised_dict()["timestamp"], str)


def test_state_read():
    s = StateRead(data={"some": "thing"}, timestamp=datetime.now())
    debug(s)
    assert s.id is None

    s = StateRead(data={"some": "thing"}, timestamp=datetime.now(), id=1)
    debug(s)
    assert s.id == 1
