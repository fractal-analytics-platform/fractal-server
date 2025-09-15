import pytest


@pytest.mark.container
async def test_admin_delete_task_group_api(
    request,
    # testdata_path,   # CHANGE THIS TO CHANGE BEHAVIOR
):
    request.getfixturevalue("fractal_ssh_list")
