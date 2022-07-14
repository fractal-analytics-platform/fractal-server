import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from devtools import debug

from fractal.tasks.dummy import dummy


def test_dummy_task():
    with NamedTemporaryFile() as tempfile:
        input_path = [Path("/fake/input/path")]
        output_path = Path(tempfile.name)

        FIRST_MESSAGE = "first run"
        dummy(
            input_paths=input_path,
            output_path=output_path,
            message=FIRST_MESSAGE,
        )

        data = json.load(tempfile)
        assert len(data) == 1
        assert data[0]["message"] == FIRST_MESSAGE

        SECOND_MESSAGE = "second run"
        dummy(
            input_paths=input_path,
            output_path=output_path,
            message=SECOND_MESSAGE,
        )

        data = json.load(tempfile)
        assert len(data) == 2
        assert data[1]["message"] == SECOND_MESSAGE

        debug(data)
