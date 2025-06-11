from pathlib import Path

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.ssh._utils import _copy_wheel_file_ssh


def test_copy_wheel_file_ssh(
    fractal_ssh: FractalSSH,
    tmp777_path: Path,
):
    path1 = tmp777_path / "path1"
    path2 = tmp777_path / "path2"
    fractal_ssh.mkdir(folder=path1.as_posix())
    fractal_ssh.mkdir(folder=path2.as_posix())
    filename = "my.whl"
    expected_archive_path = (path1 / filename).as_posix()
    current_archive_path = (path2 / filename).as_posix()
    fractal_ssh.run_command(cmd=f"touch {current_archive_path}")

    assert Path(current_archive_path).exists()
    assert not Path(expected_archive_path).exists()
    _copy_wheel_file_ssh(
        task_group=TaskGroupV2(
            path=path1.as_posix(),
            archive_path=current_archive_path,
        ),
        fractal_ssh=fractal_ssh,
        logger_name="logger",
    )
    assert Path(expected_archive_path).exists()
