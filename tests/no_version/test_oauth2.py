import threading
import time
from pathlib import Path

import uvicorn
import yaml
from fastapi import FastAPI
from httpx import AsyncClient

from fractal_server.config import OAuthClientConfig
from fractal_server.main import collect_routers


async def test_authentication(
    testdata_path: Path, override_settings_factory, db
):

    with open(testdata_path / "oauth/config.yaml") as f:
        config = yaml.safe_load(f)

    redirect_uri = config["staticClients"][0]["redirectURIs"][0]
    override_settings_factory(
        OAUTH_CLIENTS_CONFIG=[
            OAuthClientConfig(
                CLIENT_NAME="TEST",
                CLIENT_ID=config["staticClients"][0]["id"],
                CLIENT_SECRET=config["staticClients"][0]["secret"],
                OIDC_CONFIGURATION_ENDPOINT=(
                    "http://127.0.0.1:5556/"
                    "dex/.well-known/openid-configuration"
                ),
                REDIRECT_URL=redirect_uri,
            )
        ],
    )

    app = FastAPI()
    collect_routers(app)
    HOST = "127.0.0.1"
    PORT = 8001

    def run_app():
        uvicorn.run(
            app,
            host=HOST,
            port=PORT,
        )

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

            res = await client.get(callback_url)
            assert res.status_code == 204

    finally:
        thread.join(timeout=2)
