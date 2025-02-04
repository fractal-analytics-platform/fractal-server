import contextlib

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_manager


async def test_server_not_available(override_settings_factory, db, caplog):
    override_settings_factory(
        FRACTAL_EMAIL_PASSWORD=(
            "gAAAAABnoQUGHMsDgLkpDtwUtrKtf9T1so44ahEXExGRceAnf097mVY1EbNuMP5fj"
            "vkndvwCwBJM7lHoSgKQkZ4VbvO9t3PJZg=="
        ),
        FRACTAL_EMAIL_PASSWORD_KEY=(
            "lp3j2FVDkzLd0Rklnzg1pHuV9ClCuDE0aGeJfTNCaW4="
        ),
        FRACTAL_EMAIL_RECIPIENTS="test@example.org",
        FRACTAL_EMAIL_SENDER="fractal@fractal.fractal",
        FRACTAL_EMAIL_SMTP_SERVER="localhost",
        FRACTAL_EMAIL_PORT="1234",
        FRACTAL_EMAIL_INSTANCE_NAME="fractal",
    )
    user = UserOAuth(
        email="user@example.org",
        hashed_password="xxxxxx",
        oauth_accounts=[
            OAuthAccount(
                oauth_name="oidc",
                access_token="abcd",
                account_id=1,
                account_email="user@oidc.org",
            ),
            OAuthAccount(
                oauth_name="google",
                access_token="1234",
                account_id=1,
                account_email="user@gmail.com",
            ),
        ],
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    import logging

    logger = logging.getLogger("fractal_server.app.security")
    logger.propagate = True

    async with contextlib.asynccontextmanager(get_user_manager)() as um:
        await um.on_after_register(user)
    assert "ERROR sending notification email" in caplog.text
    assert "[Errno 111] Connection refused" in caplog.text
