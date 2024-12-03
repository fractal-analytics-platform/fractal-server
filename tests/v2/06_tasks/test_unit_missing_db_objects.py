from fractal_server.tasks.v2.local import collect_local
from fractal_server.tasks.v2.local import deactivate_local
from fractal_server.tasks.v2.local import reactivate_local
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh


def test_unit_missing_objects(db, caplog):
    """
    Test a branch which is in principle unreachable.
    """
    for function in [
        collect_local,
        deactivate_local,
        reactivate_local,
    ]:
        caplog.clear()
        assert caplog.text == ""
        if function == collect_local:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                wheel_buffer=None,
                wheel_filename=None,
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
    ]:
        caplog.clear()
        assert caplog.text == ""
        if function == collect_ssh:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                fractal_ssh=None,
                tasks_base_dir="/invalid",
                wheel_buffer=None,
                wheel_filename=None,
            )
        else:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                fractal_ssh=None,
                tasks_base_dir="/invalid",
            )
        assert "Cannot find database rows" in caplog.text
