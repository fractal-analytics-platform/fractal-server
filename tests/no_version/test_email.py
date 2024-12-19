import contextlib

from fractal_server.app.models import OAuthAccount
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import get_user_manager


async def test_server_not_available(override_settings_factory, db, caplog):
    override_settings_factory(
        FRACTAL_EMAIL_SETTINGS=(
            "gAAAAABnYvLgoSeECnrXlv1UoP4D_c9Of0xmwMJVopBA3TIDjOvx6YDVfe2ULz8yG"
            "r8Ba5Id8rRLjCXa_Ys8iHjvuniJyvsX0mDrc3IGSoofMEeeSCvYEe4iSWLeb_qTNV"
            "NPc4IT2-SLB-F7dEvkwzyAFnEm9dVmApd4_lQLm9_wJoS-tz1Q1K8E1_jJSgpfGgw"
            "HaINHICVh1UL_qHjIa3DwFvDPvt32tLLBZTL7oN88A8RCmg00ThIZs4HN7OQkvfni"
            "nfOiM060Lb-AeNViCVgBX-bIPWZaeQ=="
        ),
        FRACTAL_EMAIL_SETTINGS_KEY=(
            "4otDt3R-8p4S97QT0gcUzynCalByypTv01YntqQ9XFk="
        ),
        FRACTAL_EMAIL_RECIPIENTS="test@example.org",
    )
    user = UserOAuth(
        email="user@example.org",
        hashed_password="xxxxxx",
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    user.oauth_accounts = [
        OAuthAccount(
            user_id=user.id,
            oauth_name="oidc",
            access_token="abcd",
            account_id=1,
            account_email="user@oidc.org",
        ),
        OAuthAccount(
            user_id=user.id,
            oauth_name="google",
            access_token="1234",
            account_id=1,
            account_email="user@gmail.com",
        ),
    ]
    db.add(user)
    await db.commit()
    await db.refresh(user)

    async with contextlib.asynccontextmanager(get_user_manager)() as um:
        await um.on_after_register(user)
