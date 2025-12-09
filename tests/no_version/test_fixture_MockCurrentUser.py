import pytest


async def test_MockCurrentUser_fixture(MockCurrentUser):
    async with MockCurrentUser() as user:
        assert user.slurm_accounts == []

    with pytest.raises(RuntimeError, match="while also providing `user_id`"):
        async with MockCurrentUser(user_id=user.id, profile_id=1):
            pass
