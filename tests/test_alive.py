from devtools import debug

from fractal_server.config import get_settings


async def test_alive(client, override_settings):
    settings = get_settings()
    debug(settings)

    res = await client.get("/api/alive/")
    data = res.json()
    assert res.status_code == 200
    assert data["alive"] is True
