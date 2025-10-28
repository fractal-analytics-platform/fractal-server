from fractal_server.config import get_data_settings
from fractal_server.config import get_db_settings
from fractal_server.config import get_email_settings
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


async def test_alive(client):
    res = await client.get("/api/alive/")
    data = res.json()
    assert res.status_code == 200
    assert data["alive"] is True


async def test_settings_endpoint(client, MockCurrentUser):
    # Unauthorized

    res = await client.get("/api/settings/app/")
    assert res.status_code == 401
    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get("/api/settings/app/")
        assert res.status_code == 401

    # Success

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get("/api/settings/app/")
        assert res.status_code == 200
        endpoint_settings = res.json()
        settings = Inject(get_settings)
        assert settings.model_dump().keys() == endpoint_settings.keys()

        res = await client.get("/api/settings/database/")
        assert res.status_code == 200
        endpoint_settings = res.json()
        settings = Inject(get_db_settings)
        assert settings.model_dump().keys() == endpoint_settings.keys()

        res = await client.get("/api/settings/email/")
        assert res.status_code == 200
        endpoint_settings = res.json()
        settings = Inject(get_email_settings)
        assert settings.model_dump().keys() == endpoint_settings.keys()

        res = await client.get("/api/settings/data/")
        assert res.status_code == 200
        endpoint_settings = res.json()
        settings = Inject(get_data_settings)
        assert settings.model_dump().keys() == endpoint_settings.keys()

        res = await client.get("/api/settings/oauth/")
        assert res.status_code == 200
        endpoint_settings = res.json()
        settings = Inject(get_oauth_settings)
        assert settings.model_dump().keys() == endpoint_settings.keys()
