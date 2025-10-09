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


def test_unit_missing_objects(
    db,
    caplog,
    local_resource_profile_objects,
    slurm_ssh_resource_profile_objects,
):
    """
    Test a branch which is in principle unreachable.
    """
    resource, profile = local_resource_profile_objects

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
                resource=resource,
                profile=profile,
            )
        elif function == collect_local_pixi:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                tar_gz_file=None,
                resource=resource,
                profile=profile,
            )
        else:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                resource=resource,
                profile=profile,
            )
        assert "Cannot find database rows" in caplog.text

    resource, profile = slurm_ssh_resource_profile_objects

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
                tasks_base_dir="/invalid",
                wheel_file=None,
                resource=resource,
                profile=profile,
            )
        elif function == collect_ssh_pixi:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                tasks_base_dir="/invalid",
                tar_gz_file=None,
                resource=resource,
                profile=profile,
            )
        else:
            function(
                task_group_activity_id=9999,
                task_group_id=9999,
                tasks_base_dir="/invalid",
                resource=resource,
                profile=profile,
            )
        assert "Cannot find database rows" in caplog.text
