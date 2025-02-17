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


async def test_unit_get_sanitized_settings():
    settings = Inject(get_settings)
    sanitized_settings = settings.get_sanitized()
    assert settings.model_dump().keys() == sanitized_settings.keys()
    for k in sanitized_settings.keys():
        if not k.upper().startswith("FRACTAL") or any(
            s in k.upper()
            for s in ["PASSWORD", "SECRET", "PWD", "TOKEN", "KEY"]
        ):
            assert sanitized_settings[k] == "***"
        else:
            assert sanitized_settings[k] == settings.model_dump()[k]


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
    for k, v in endpoint_settings.items():
        if v != "***":
            assert v == settings[k]
