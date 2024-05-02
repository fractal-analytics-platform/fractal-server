from common.common import full_workflow


async def test_full_workflow(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    monkey_slurm,
    relink_python_interpreter_v2,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm",
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts-slurm",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
    )
