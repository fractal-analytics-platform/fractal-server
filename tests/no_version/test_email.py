import logging

from fractal_server.app.security.signup_email import (
    send_fractal_email_or_log_failure,
)
from fractal_server.config._email import PublicEmailSettings


async def test_send_fractal_email_or_log_failure(caplog, monkeypatch):
    email_settings_public = PublicEmailSettings(
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

    # Mock the logger, so that `propagate=True`
    import fractal_server.app.security.signup_email

    _logger = logging.getLogger("some-logger")
    monkeypatch.setattr(
        fractal_server.app.security.signup_email, "logger", _logger
    )

    caplog.clear()
    send_fractal_email_or_log_failure(
        subject="subject",
        msg="msg",
        email_settings=None,
    )
    assert "Cannot send email" in caplog.text
    assert "email_settings=None" in caplog.text

    caplog.clear()
    send_fractal_email_or_log_failure(
        subject="subject", msg="msg", email_settings=email_settings_public
    )
    assert "Could not send self-registration email" in caplog.text
    assert "[Errno 111] Connection refused" in caplog.text
