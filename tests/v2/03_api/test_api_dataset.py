from devtools import debug


async def test_api_dataset_v2(client, MockCurrentUser):

    async with MockCurrentUser():

        res = await client.post("api/v2/project/", json=dict(name="projectV2"))
        debug(res.json())
        assert res.status_code == 201
        projectV2 = res.json()
        p2_id = projectV2["id"]

        # POST

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/", json=dict(name="dataset")
        )
        assert res.status_code == 201
        dataset1 = res.json()

        res = await client.post(
            f"api/v2/project/{p2_id}/dataset/",
            json=dict(
                name="dataset",
                images=[
                    {
                        "path": "/tmp/xxx.yz",
                        "attributes": {"x": 10},
                    },
                    {
                        "path": "/tmp/xxx_corr.yz",
                        "attributes": {"x": 10, "y": True, "z": 3.14},
                    },
                ],
                filters={"x": 10},
            ),
        )
        assert res.status_code == 201
        dataset2 = res.json()

        # GET (3 different ones)

        # 1
        res = await client.get("api/v2/dataset/")
        assert res.status_code == 200
        user_dataset_list = res.json()

        # 2
        res = await client.get(f"api/v2/project/{p2_id}/dataset/")
        assert res.status_code == 200
        project_dataset_list = res.json()

        # 3
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset1['id']}/"
        )
        assert res.status_code == 200
        ds1 = res.json()
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.status_code == 200
        ds2 = res.json()

        assert user_dataset_list == project_dataset_list == [ds1, ds2]

        # UPDATE

        NEW_NAME = "new name"
        res = await client.patch(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/",
            json=dict(name=NEW_NAME),
        )
        assert res.status_code == 200
        res = await client.get(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.json()["name"] == NEW_NAME

        # DELETE

        res = await client.delete(
            f"api/v2/project/{p2_id}/dataset/{dataset2['id']}/"
        )
        assert res.status_code == 204
        res = await client.get(f"api/v2/project/{p2_id}/dataset/")
        assert len(res.json()) == 1
