import json
import sys
from pathlib import Path

from fractal_server import __VERSION__
from fractal_server.app.runner.executors.slurm_common.remote import worker


def test_slurm_remote(tmp_path: Path):
    in_fname = (tmp_path / "in.json").as_posix()
    log_path = (tmp_path / "log.txt").as_posix()
    metadiff_path = (tmp_path / "metadiff.json").as_posix()

    RESULT = dict(this="nice")
    with open(metadiff_path, "w") as f:
        json.dump(RESULT, f)

    with open(in_fname, "w") as f:
        json.dump(
            dict(
                python_version=tuple(sys.version_info[:3]),
                fractal_server_version=__VERSION__,
                metadiff_file_remote=metadiff_path,
                log_file_remote=log_path,
                full_command="echo --in-json xxx --out-json yyy",
            ),
            f,
        )

    # CASE 1: metadiff file exists
    out_fname = (tmp_path / "subdir2/out_1.json").as_posix()
    worker(in_fname=in_fname, out_fname=out_fname)
    with open(out_fname) as f:
        success, result = json.load(f)
        assert success
        assert result == RESULT

    with open(log_path) as f:
        assert f.read() == "--in-json xxx --out-json yyy\n"

    # CASE 2: metadiff file does not exist
    Path(metadiff_path).unlink()
    out_fname = (tmp_path / "subdir2/out_2.json").as_posix()
    worker(in_fname=in_fname, out_fname=out_fname)
    with open(out_fname) as f:
        success, result = json.load(f)
        assert success
        assert result is None

    # CASE 3: fractal-server version mismatch
    with open(in_fname, "w") as f:
        json.dump(
            dict(
                python_version=tuple(sys.version_info[:3]),
                fractal_server_version="0.0.1",
                metadiff_file_remote=metadiff_path,
                log_file_remote=log_path,
                full_command="echo --in-json xxx --out-json yyy",
            ),
            f,
        )
    out_fname = (tmp_path / "subdir2/out_3.json").as_posix()
    worker(in_fname=in_fname, out_fname=out_fname)
    with open(out_fname) as f:
        success, exc_proxy = json.load(f)
        assert not success
        assert exc_proxy["exc_type_name"] == "FractalVersionMismatch"
        assert "traceback_string" in exc_proxy.keys()

    # CASE 4: python version mismatch is not an error
    with open(in_fname, "w") as f:
        json.dump(
            dict(
                python_version=(4, 0, 0),
                fractal_server_version=__VERSION__,
                metadiff_file_remote=metadiff_path,
                log_file_remote=log_path,
                full_command="echo --in-json xxx --out-json yyy",
            ),
            f,
        )
    out_fname = (tmp_path / "subdir2/out_3.json").as_posix()
    worker(in_fname=in_fname, out_fname=out_fname)
    with open(out_fname) as f:
        success, exc_proxy = json.load(f)
        assert success
