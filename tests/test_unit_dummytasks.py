import asyncio
import json
from pathlib import Path

from devtools import debug

from .data import tasks_dummy as tasks_package
from .data.tasks_dummy import dummy as dummy_module
from .data.tasks_dummy.dummy import dummy
from .data.tasks_dummy.dummy_parallel import dummy_parallel
from fractal_server.common.schemas.manifest import ManifestV1


FIRST_TEST_MESSAGE = "first call"
SECOND_TEST_MESSAGE = "second call"
ERROR_MESSAGE = "This is an error message"


def test_dummy_direct_call(tmp_path):
    out_path = tmp_path / "output"
    metadata_update = dummy(
        input_paths=[str(tmp_path)],
        output_path=str(out_path),
        metadata={"before": "test"},
        message=FIRST_TEST_MESSAGE,
        index=0,
    )
    assert out_path.exists()
    assert metadata_update == {"dummy": "dummy 0", "index": ["0", "1", "2"]}
    debug(out_path)
    with (out_path / "0.result.json").open("r") as f:
        data = json.load(f)
    debug(data)

    assert len(data) == 1
    assert data[0]["message"] == FIRST_TEST_MESSAGE

    # Second call
    metadata_update = dummy(
        input_paths=[str(tmp_path)],
        output_path=str(out_path),
        metadata={"before": "test"},
        message=SECOND_TEST_MESSAGE,
        index=1,
    )
    assert metadata_update == {"dummy": "dummy 1", "index": ["0", "1", "2"]}
    with (out_path / "1.result.json").open("r") as f:
        data = json.load(f)
    debug(data)

    assert len(data) == 1
    assert data[0]["message"] == SECOND_TEST_MESSAGE


async def test_dummy_process_call(tmp_path):
    out_path = tmp_path / "output"
    args = dict(
        input_paths=[str(tmp_path)],
        output_path=str(out_path),
        metadata={"before": "test"},
        message=FIRST_TEST_MESSAGE,
        index=0,
    )
    args_file = tmp_path / "args.json"
    with args_file.open("w") as fargs:
        json.dump(args, fargs)

    cmd = f"python {dummy_module.__file__} -j {args_file}"
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    debug(proc.returncode)
    debug(stdout)
    debug(stderr)
    metadata_update = json.loads(stdout)
    assert proc.returncode == 0

    assert out_path.exists()
    assert metadata_update == {"dummy": "dummy 0", "index": ["0", "1", "2"]}
    with (out_path / "0.result.json").open("r") as f:
        data = json.load(f)
    debug(data)


async def test_dummy_fail_process_call(tmp_path):
    out_path = tmp_path / "output"
    args = dict(
        input_paths=[str(tmp_path)],
        output_path=str(out_path),
        metadata={"before": "test"},
        message=ERROR_MESSAGE,
        raise_error=True,
    )
    args_file = tmp_path / "args.json"
    with args_file.open("w") as fargs:
        json.dump(args, fargs)

    cmd = f"python {dummy_module.__file__} -j {args_file}"
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    debug(proc.returncode)
    debug(stdout)
    debug(stderr)
    assert proc.returncode == 1
    assert f"ValueError: {ERROR_MESSAGE}" in str(stderr)


def test_dummy_parallel_direct_call(tmp_path):
    list_components = ["A", "B", "C"]
    out_path = tmp_path / "output"

    for component in list_components:
        metadata_update = dummy_parallel(
            input_paths=[str(tmp_path)],
            output_path=str(out_path),
            component=component,
            metadata={"before": "test"},
            message=FIRST_TEST_MESSAGE,
        )
        assert metadata_update == {"test_parallel": 1}

    assert out_path.exists()
    out_files = list(out_path.glob("*"))
    debug(out_files)
    assert len(out_files) == len(list_components)

    for out_file in out_files:
        with out_file.open("r") as fin:
            data = json.load(fin)
        assert out_file.name == f'{data["component"]}.result.json'
        assert data["message"] == FIRST_TEST_MESSAGE


def test_manifest_validation():
    manifest_path = (
        Path(tasks_package.__file__).parent / "__FRACTAL_MANIFEST__.json"
    )
    with manifest_path.open("r") as f:
        manifest_dict = json.load(f)

    if manifest_dict["manifest_version"] == 1:
        manifest_obj = ManifestV1(**manifest_dict)
    debug(manifest_obj)
