import pytest

from fractal_server.runner.executors.slurm_sudo.runner import SlurmSudoRunner


@pytest.mark.container
async def test_check_fractal_server_versions_executable(
    tmp777_path,
    monkey_slurm,
    monkeypatch,
    slurm_sudo_resource_profile_db,
):
    resource, profile = slurm_sudo_resource_profile_db[:]
    with SlurmSudoRunner(
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache").as_posix(),
        resource=resource,
        profile=profile,
        fractal_job_id=99,
        resource_id=resource.id,
    ) as runner:
        # Successful check
        runner.check_fractal_server_versions()

        # Set up mock
        def patched_json_loads(*args, **kwargs):
            return dict(fractal_server="9.9.9")

        monkeypatch.setattr("json.loads", patched_json_loads)

        # Failed check
        with pytest.raises(RuntimeError, match="version mismatch"):
            runner.check_fractal_server_versions()
