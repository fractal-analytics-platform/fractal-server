async def test_admin_get_projects(
    client,
    MockCurrentUser,
    project_factory,
):
    async with MockCurrentUser(user_email="y@example.org") as user1:
        project1 = await project_factory(user=user1, name="ccc")
        project2 = await project_factory(user=user1, name="bbb")

    async with MockCurrentUser(user_email="x@example.org") as user2:
        project3 = await project_factory(user=user2, name="aaa")

    async with MockCurrentUser(is_superuser=True):
        # no query params
        res = await client.get("/admin/v2/project/")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 3
        assert res.json()["current_page"] == 1

        projects = res.json()["items"]

        assert projects[0]["user_email"] == user2.email
        assert projects[0]["id"] == project3.id

        assert projects[1]["user_email"] == user1.email
        assert projects[1]["id"] == project2.id

        assert projects[2]["user_email"] == user1.email
        assert projects[2]["id"] == project1.id

        # pagination query params
        res = await client.get("/admin/v2/project/?page_size=2&page=2")
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 2
        assert res.json()["current_page"] == 2

        assert len(res.json()["items"]) == 1
        assert res.json()["items"][0]["user_email"] == user1.email
        assert res.json()["items"][0]["id"] == project1.id

        # project_id
        res = await client.get(f"/admin/v2/project/?project_id={project1.id}")
        assert res.json()["items"][0]["user_email"] == user1.email
        assert res.json()["items"][0]["id"] == project1.id

        # name
        res = await client.get("/admin/v2/project/?name=B")
        assert res.json()["items"][0]["user_email"] == user1.email
        assert res.json()["items"][0]["id"] == project2.id

        # user_email
        res = await client.get(f"/admin/v2/project/?user_email={user1.email}")
        assert res.json()["total_count"] == 2
        assert res.json()["items"][0]["user_email"] == user1.email
        assert res.json()["items"][0]["id"] == project2.id
        assert res.json()["items"][1]["user_email"] == user1.email
        assert res.json()["items"][1]["id"] == project1.id
