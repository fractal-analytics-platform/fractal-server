import contextlib

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_manager
from fractal_server.config._email import PublicEmailSettings


async def test_server_not_available(
    override_email_settings_factory, db, caplog
):
    override_email_settings_factory(
        public=PublicEmailSettings(
            sender="fractal@fractal.fractal",
            recipients=["test@example.org"],
            smtp_server="localhost",
            port=1234,
            encrypted_password=(
                "gAAAAABnoQUGHMsDgLkpDtwUtrKtf9T1so44ahEXExGRceAnf097mVY1EbNuM"
                "P5fjvkndvwCwBJM7lHoSgKQkZ4VbvO9t3PJZg=="
            ),
            encryption_key="lp3j2FVDkzLd0Rklnzg1pHuV9ClCuDE0aGeJfTNCaW4=",
            instance_name="fractal",
            use_starttls=False,
            use_login=True,
        )
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
