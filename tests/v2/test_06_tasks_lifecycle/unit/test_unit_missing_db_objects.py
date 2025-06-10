from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.v2.local import collect_local
from fractal_server.tasks.v2.local import collect_local_pixi
from fractal_server.tasks.v2.local import deactivate_local
from fractal_server.tasks.v2.local import deactivate_local_pixi
from fractal_server.tasks.v2.local import reactivate_local
from fractal_server.tasks.v2.local import reactivate_local_pixi
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.ssh import collect_ssh_pixi
from fractal_server.tasks.v2.ssh import deactivate_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh_pixi
from fractal_server.tasks.v2.ssh import reactivate_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh_pixi


def test_unit_missing_objects(db, caplog):
    """
    Test a branch which is in principle unreachable.
    """
    for function in [
        collect_local,
        deactivate_local,
        reactivate_local,
        collect_local_pixi,
        deactivate_local_pixi,
        reactivate_local_pixi,
    ]:
        caplog.clear()
        assert caplog.text == ""
        if function == collect_local:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                wheel_file=None,
            )
        elif function == collect_local_pixi:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                tar_gz_file=None,
            )
        else:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
            )
        assert "Cannot find database rows" in caplog.text

    for function in [
        collect_ssh,
        deactivate_ssh,
        reactivate_ssh,
        collect_ssh_pixi,
        deactivate_ssh_pixi,
        reactivate_ssh_pixi,
    ]:
        caplog.clear()
        assert caplog.text == ""
        if function == collect_ssh:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                ssh_config=SSHConfig(
                    host="fake",
                    user="fake",
                    key_path="fake",
                ),
                tasks_base_dir="/invalid",
                wheel_file=None,
            )
        elif function == collect_ssh_pixi:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                ssh_config=SSHConfig(
                    host="fake",
                    user="fake",
                    key_path="fake",
                ),
                tasks_base_dir="/invalid",
                tar_gz_file=None,
            )
        else:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                ssh_config=SSHConfig(
                    host="fake",
                    user="fake",
                    key_path="fake",
                ),
                tasks_base_dir="/invalid",
            )
        assert "Cannot find database rows" in caplog.text
