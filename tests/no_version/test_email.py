import contextlib

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_manager


async def test_server_not_available(override_settings_factory, db, caplog):
    override_settings_factory(
        FRACTAL_EMAIL_SETTINGS=(
            "gAAAAABnoPQV82lnV1OlIXnOr9Nk-29VTzg6prP6o-bUEMu02wXImFLjKOZdbW9EA"
            "YMgOXdk3FkGJTfIDXgstl26nYFQAhKOfhYnggpbXakudPA9szG9BsmEiHpWCxPjTJ"
            "YR0IoCJhx84c3B06OxNnOjNFHkZKdiXnMwX3W90VSSTawffdxEIeh9vGUi51UM5z9"
            "FZLWxqNBMMIN1X02nKn5IJWhOu8Pzx97LTPxEXnuKbKgrsDX2xFBfj1I3ZAX3Fw90"
            "qXcojMymNZw9yrS2y6s7h4bmFXVU84x-YVtBRZb99rU-RXkIAhQ="
        ),
        FRACTAL_EMAIL_SETTINGS_KEY=(
            "8hUJTuN6h6DOqZ2_2n-oOkuHGeTSscK7bqn8GMUzWQU="
        ),
        FRACTAL_EMAIL_RECIPIENTS="test@example.org",
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
