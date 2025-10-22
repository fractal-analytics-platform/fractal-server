async def test_MockCurrentUser_fixture(MockCurrentUser):
    async with MockCurrentUser() as user:
        assert user.slurm_accounts == []
