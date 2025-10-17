import pytest
from devtools import debug


@pytest.mark.parametrize("slurm_user", ("test01", None))
async def test_MockCurrentUser_fixture(
    MockCurrentUser,
    slurm_user,
):
    user_settings_dict = dict(slurm_user=slurm_user)
    async with MockCurrentUser(user_settings_dict=user_settings_dict) as user:
        debug(user)
        assert user.settings.slurm_user == slurm_user
