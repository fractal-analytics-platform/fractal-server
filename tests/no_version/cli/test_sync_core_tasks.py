import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from fractal_server.cli._sync_core_tasks import _get_final_list


def test__get_final_list(tmp_path: Path):
    path1 = tmp_path / "1.json"
    path2 = tmp_path / "2.json"
    path3 = tmp_path / "3.json"

    with pytest.raises(ValidationError):
        path1.write_text(json.dumps([["a", "b"]]))
        _get_final_list(main_list_path=path1)

    path1.write_text(json.dumps([["a", "b", "c"], ["a", "b", "c"]]))
    assert _get_final_list(main_list_path=path1) == {
        ("a", "b", "c"),
    }

    with pytest.raises(ValidationError):
        path1.write_text(json.dumps([["a", "b", "c"], [1, 2, 3, 4]]))
        _get_final_list(main_list_path=path1)

    path1.write_text(
        json.dumps(
            [
                ["pkg1", "1.0.0", "A"],
                ["pkg1", "1.0.0", "B"],
                ["pkg1", "1.0.0", "C"],
                ["pkg2", "1.2.3", "A"],
                ["pkg2", "1.2.3", "D"],
            ]
        )
    )
    path2.write_text(
        json.dumps(
            [
                ["pkg2", "1.2.3", "D"],
                ["pkg3", "1.2.3", "D"],
                ["pkg3", "1.2.3", "E"],
            ]
        )
    )
    path3.write_text(
        json.dumps(
            [
                ["pkg1", "1.0.0", "A"],
                ["pkg1", "1.0.0", "B"],
            ]
        )
    )
    final_list = _get_final_list(
        main_list_path=path1,
        custom_list_path=path2,
        ignore_list_path=path3,
    )
    print(final_list)
    assert final_list == {
        ("pkg2", "1.2.3", "A"),
        ("pkg3", "1.2.3", "D"),
        ("pkg2", "1.2.3", "D"),
        ("pkg1", "1.0.0", "C"),
        ("pkg3", "1.2.3", "E"),
    }
