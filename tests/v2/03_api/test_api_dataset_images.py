from typing import Any

from devtools import debug

from fractal_server.images import SingleImage

PREFIX = "api/v2"

IMAGES = [
    SingleImage(path="/A", attributes={"a": 1, "b": 2, "c": 3}),
    SingleImage(path="/A", attributes={"x": 1, "y": 2, "z": 3}),
    SingleImage(path="/A", attributes={"x": 1, "b": 2, "c": 3}),
    SingleImage(path="/A", attributes={"a": 1, "y": False, "c": 3}),
    SingleImage(path="/B", attributes={"a": "AAA", "b": 2, "z": -3}),
    SingleImage(path="/B", attributes={"a": "AAA", "b": 2, "z": 0}),
    SingleImage(path="/C", attributes={"b": None}),
]


def encode_dict(dict_to_encode: dict[str, Any]) -> str:
    # %7B --> {
    # %7D --> }
    # %22 --> "
    url = "%7B"
    for k, v in dict_to_encode.items():
        if isinstance(v, str):
            url += f"%22{k}%22:%22{v}%22,"
        elif v is None:
            url += f"%22{k}%22:null,"
        elif v is True:
            url += f"%22{k}%22:true,"
        elif v is False:
            url += f"%22{k}%22:false,"
        else:
            url += f"%22{k}%22:{v},"
    url = url[:-1]
    url += "%7D"
    return url


async def test_get_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)
    debug(dataset)

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
    )
    assert "images" not in res.json()
    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )

    assert res.status_code == 200
    assert len(res.json()["images"]) == len(IMAGES)
    assert sorted(res.json()["attributes"]) == ["a", "b", "c", "x", "y", "z"]

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?path=/A",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 4

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes={encode_dict(dict(A=123))}",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 0

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?path=/A&attributes={encode_dict(dict(a=1))}",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 2

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes={encode_dict(dict(x=1,y=2,z=3))}",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 1

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes={encode_dict(dict(a='AAA',z=0))}",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 1

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes={encode_dict(dict(b=None))}",
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == len(IMAGES)  # None filters are ignored

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes={encode_dict(dict(x=[1,2,3]))}",
    )
    assert res.status_code == 422
    assert "scalar dict" in res.json()["detail"]

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        f"?attributes=xyz",
    )
    assert res.status_code == 422
    assert "valid dict" in res.json()["detail"]


async def test_delete_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)
    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )
    assert len(res.json()["images"]) == len(IMAGES)

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )
    assert len(res.json()["images"]) == len(IMAGES)

    res = await client.delete(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )
    assert res.status_code == 422
    assert "not a valid list" in res.json()["detail"][0]["msg"]

    res = await client.delete(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        "?path=/A&path=/def"
    )
    assert res.status_code == 422
    assert "not a sublist" in res.json()["detail"]

    res = await client.delete(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
        "?path=/A&path=/B"
    )
    assert res.status_code == 204
    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )
    assert len(res.json()["images"]) == 1
    assert res.json()["images"][0]["path"] == "/C"
