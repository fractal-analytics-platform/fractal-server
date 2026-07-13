import pytest

from fractal_server.cli._parser import get_parser


def test_parser(db, capsys):
    parser = get_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(args=["invalid-command"])
    args = parser.parse_args(args=["recent"])
    assert args.cmd == "recent"
