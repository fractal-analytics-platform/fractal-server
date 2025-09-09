from fractal_server.ssh._fabric import SSHConfig


def delete_ssh(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    pass
