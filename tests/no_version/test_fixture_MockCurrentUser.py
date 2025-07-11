import pytest
from devtools import debug


@pytest.mark.parametrize("username", ("my_username", None))
@pytest.mark.parametrize("slurm_user", ("test01", None))
async def test_MockCurrentUser_fixture(
    MockCurrentUser,
    username,
    slurm_user,
):
    user_kwargs = dict(username=username)
    user_settings_dict = dict(slurm_user=slurm_user)
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        debug(user)
        assert user.username == username
        assert user.settings.slurm_user == slurm_user
