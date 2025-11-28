import pytest
from fastapi import HTTPException
from fastapi_users.exceptions import UserAlreadyExists

from fractal_server.app.db import get_async_db
from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_db
from fractal_server.app.security import get_user_manager
from fractal_server.config._email import PublicEmailSettings


async def test_oauth_callback(
    override_settings_factory, override_email_settings_factory, db
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
    HELP_PAGE = "https://example.org/fractal-help-page"
    override_settings_factory(FRACTAL_HELP_URL=HELP_PAGE)
    user = UserOAuth(
        email="user1@example.org",
        hashed_password="xxx",
        project_dir=["/fake"],
        oauth_accounts=[
            OAuthAccount(
                oauth_name="oidc",
                access_token="abcd",
                account_id=1,
                account_email="user1-oidc@example.org",
            ),
            OAuthAccount(
                oauth_name="google",
                access_token="1234",
                account_id=1,
                account_email="user1-google@example.org",
            ),
        ],
    )
    db.add(user)
    db.add(
        UserOAuth(
            email="user2@example.org",
            hashed_password="xxx",
            project_dir=["/fake"],
        )
    )
    await db.commit()
    await db.close()

    async for db_session in get_async_db():
        async for user_db in get_user_db(db_session):
            async for um in get_user_manager(user_db=user_db):
                # User is found via `um.get_by_oauth_account`
                await um.oauth_callback(
                    account_id=1,
                    oauth_name="oidc",
                    access_token="fake",
                    account_email="user1@example.org",
                    associate_by_email=True,
                )

                # User is not found via `um.get_by_oauth_account` (due to
                # `oauth_name` mismatch), email already exists, association
                # takes place
                await um.oauth_callback(
                    account_id=1,
                    oauth_name="aaaa",
                    access_token="fake",
                    account_email="user1@example.org",
                    associate_by_email=True,
                )

                # User is not found via `um.get_by_oauth_account` (due to
                # `oauth_name` mismatch), email already exists, association
                # cannot take place and thus lead to an exception
                with pytest.raises(UserAlreadyExists):
                    await um.oauth_callback(
                        account_id=1,
                        oauth_name="bbbb",
                        access_token="fake",
                        account_email="user1@example.org",
                        associate_by_email=False,
                    )

                # Failure and 422
                with pytest.raises(HTTPException) as exc_info:
                    await um.oauth_callback(
                        oauth_name="x",
                        access_token="x",
                        account_id="x",
                        account_email="xxx@example.org",
                    )
                assert HELP_PAGE in str(exc_info.value)
