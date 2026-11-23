from argparse import Namespace

import pytest

from fractal_server.cli.__main__ import run


def test_invalid_command(monkeypatch):
    import fractal_server.cli.__main__

    def _mocked_parse_args() -> Namespace:
        return Namespace(cmd="fake-command")

    monkeypatch.setattr(
        fractal_server.cli.__main__, "parse_args", _mocked_parse_args
    )
    with pytest.raises(SystemExit, match="invalid command"):
        run()
