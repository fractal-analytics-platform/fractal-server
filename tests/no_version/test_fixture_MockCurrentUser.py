import pytest
from devtools import debug


@pytest.mark.parametrize("cache_dir", ("/some/path", None))
@pytest.mark.parametrize("username", ("my_username", None))
@pytest.mark.parametrize("slurm_user", ("test01", None))
async def test_MockCurrentUser_fixture(
    MockCurrentUser,
    cache_dir,
    username,
    slurm_user,
):

    user_kwargs = dict(username=username)
    user_settings_dict = dict(cache_dir=cache_dir, slurm_user=slurm_user)
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        debug(user)
        assert user.username == username
        assert user.settings.cache_dir == cache_dir
        assert user.settings.slurm_user == slurm_user
