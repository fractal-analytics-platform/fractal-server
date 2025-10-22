from devtools import debug


async def test_MockCurrentUser_fixture(
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        debug(user)
        # FIXME: add some assertion here
        # assert user.settings.slurm_user == slurm_user
