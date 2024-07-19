import threading
import time
from pathlib import Path

import uvicorn
import yaml
from fastapi import FastAPI
from httpx import AsyncClient
from sqlmodel import select

from fractal_server.app.models.security import OAuthAccount
from fractal_server.app.models.security import UserOAuth
from fractal_server.config import OAuthClientConfig
from fractal_server.main import collect_routers


async def test_authentication(
    db,
    override_settings_factory,
    testdata_path: Path,
):
    res = await db.execute(select(UserOAuth))
    assert len(res.unique().all()) == 0
    res = await db.execute(select(OAuthAccount))
    assert len(res.unique().all()) == 0

    with open(testdata_path / "oauth/config.yaml") as f:
        oauth_config = yaml.safe_load(f)

    redirect_uri = oauth_config["staticClients"][0]["redirectURIs"][0]
    override_settings_factory(
        OAUTH_CLIENTS_CONFIG=[
            OAuthClientConfig(
                CLIENT_NAME="TEST",
                CLIENT_ID=oauth_config["staticClients"][0]["id"],
                CLIENT_SECRET=oauth_config["staticClients"][0]["secret"],
                OIDC_CONFIGURATION_ENDPOINT=(
                    "http://127.0.0.1:5556/"
                    "dex/.well-known/openid-configuration"
                ),
                REDIRECT_URL=redirect_uri,
            )
        ],
    )
    app = FastAPI()
    app.state.jobsV1 = []
    app.state.jobsV2 = []
    app.state.fractal_ssh = None
    collect_routers(app)

    HOST = "127.0.0.1"
    PORT = 8001

    def run_app():
        uvicorn.run(app, host=HOST, port=PORT)

    thread = threading.Thread(target=run_app, daemon=True)
    thread.start()
    time.sleep(2)

    try:

        async with AsyncClient(base_url=f"http://{HOST}:{PORT}") as client:

            res = await client.get("auth/test/authorize/")
            assert res.status_code == 200

            authorization_url = res.json()["authorization_url"]

            res = await client.get(authorization_url)

            container_location = "/".join(authorization_url.split("/")[:3])
            redirect_url = res.headers["location"]

            res = await client.get(f"{container_location}{redirect_url}")
            res = await client.get(res.headers["location"])

            callback_url = res.headers["location"].split(str(PORT))[1]

            res = await db.execute(select(UserOAuth))
            assert len(res.unique().all()) == 0
            res = await db.execute(select(OAuthAccount))
            assert len(res.all()) == 0

            res = await client.get(callback_url)
            assert res.status_code == 204

            res = await db.execute(select(UserOAuth))
            users = res.unique().all()[0]
            assert len(users) == 1

            user: UserOAuth = users[0]
            assert user.email == "kilgore@kilgore.trout"
            assert len(user.oauth_accounts) == 1

            res = await db.execute(select(OAuthAccount))
            oauth_accounts = res.all()[0]
            assert len(oauth_accounts) == 1

            oauth_account: OAuthAccount = oauth_accounts[0]
            assert oauth_account.oauth_name == "openid"
            assert oauth_account is user.oauth_accounts[0]

    finally:
        thread.join(timeout=2)
