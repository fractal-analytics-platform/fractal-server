from pathlib import Path

from devtools import debug

from fractal_server.config import get_settings
from fractal_server.syringe import Inject


async def test_alive(client, override_settings):
    settings = Inject(get_settings)
    debug(settings)

    res = await client.get("/api/alive/")
    data = res.json()
    assert res.status_code == 200
    assert data["alive"] is True


async def test_settings_endpoint(client, MockCurrentUser):

    settings = Inject(get_settings).model_dump()
    for k, v in settings.items():
        if isinstance(v, Path):
            settings[k] = v.as_posix()  # the client returns strings, not Paths

    res = await client.get("/api/settings/")
    assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get("/api/settings/")
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get("/api/settings/")
        assert res.status_code == 200

    endpoint_settings = res.json()

    assert settings.keys() == endpoint_settings.keys()

    obfuscated = []
    for k, v in endpoint_settings.items():
        if "***" not in str(v):
            assert v == settings[k]
        else:
            obfuscated.append(k)
    assert "JWT_SECRET_KEY" in obfuscated
    assert "POSTGRES_PASSWORD" in obfuscated
    assert "FRACTAL_DEFAULT_ADMIN_PASSWORD" in obfuscated
